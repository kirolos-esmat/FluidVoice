import Foundation

/// One reconciler owns all media queries and commands. Recording events update
/// intent synchronously; they never wait for media control or delay first PCM.

/// What `MediaPlaybackService` did at the start of a transcription session.
/// Stored on `ASRService` so the matching restore at stop knows whether to
/// resume playback, restore the system volume, or do nothing.
///
/// The duck case carries a `SystemVolumeSnapshot` rather than a single
/// `Float` so output devices that expose only per-channel volume (no master)
/// can have their stereo balance restored exactly. A flat scalar would
/// collapse L/R to their average on every duck cycle.
enum MediaSessionAction: Equatable {
    case none
    case paused
    case ducked(previousVolume: SystemVolumeSnapshot)
}

/// Default volume the system output is dropped to while ducking if not configured in settings.
private let kDefaultDuckTargetVolume: Float = 0.15

/// Length of the fade ramp in seconds. Short enough that the duck has
/// fully landed before the user starts dictating, long enough to read as a
/// fade rather than a hard cut.
private let kFadeDuration: TimeInterval = 0.1

/// Number of discrete steps in the fade ramp. 30 steps over 100ms is 300 Hz,
/// well above the threshold where you'd hear the staircase. The fade only
/// covers the second half of the duck (the first half is a synchronous snap
/// in `duckSystemVolume()` for snappy feel) so a relaxed 100ms tail reads
/// as a soft landing rather than a long fade.
private let kFadeSteps = 30
@MainActor
final class MediaPlaybackService {
    static let shared = MediaPlaybackService(transport: MediaPlaybackProcessTransport())

    private struct Session {
        let id: Int
        var mayPause: Bool
    }

    private let transport: any MediaPlaybackTransport
    private let settle: @Sendable () async -> Void
    private let now: @Sendable () -> TimeInterval
    private var session: Session?
    private var revision: UInt64 = 0
    private var attemptedSession: Int?
    private var pausedTarget: MediaPlaybackSnapshot?
    private var worker: Task<Void, Never>?
    private var suspendedUntil: TimeInterval = 0
    private var isShuttingDown = false
    /// Holds the in-flight volume-fade task so a new fade can cancel any
    /// previous one (e.g. the user releases the hotkey before the
    /// fade-down has finished, and the fade-up needs to take over cleanly).
    private var activeFadeTask: Task<Void, Never>?

    init(
        transport: any MediaPlaybackTransport,
        settle: @escaping @Sendable () async -> Void = {
            try? await Task.sleep(nanoseconds: 150_000_000)
        },
        now: @escaping @Sendable () -> TimeInterval = { ProcessInfo.processInfo.systemUptime }
    ) {
        self.transport = transport
        self.settle = settle
        self.now = now
    }

    func recordingStarted(sessionID: Int, enabled: Bool) {
        guard !self.isShuttingDown else { return }
        self.session = enabled ? Session(id: sessionID, mayPause: true) : nil
        self.revision &+= 1
        self.log("recording_started session=\(sessionID) enabled=\(enabled)")
        self.wake()
    }

    /// Prevent a slow query from issuing pause after the hotkey is released.
    /// A previously confirmed pause stays owned until transcription finishes.
    func recordingStopped(sessionID: Int) {
        guard self.session?.id == sessionID else { return }
        self.session?.mayPause = false
        self.revision &+= 1
        self.log("recording_stopped session=\(sessionID)")
        self.wake()
    }

    /// An older transcription finishing cannot resume a newer recording's media.
    func sessionFinished(sessionID: Int) {
        guard self.session?.id == sessionID else { return }
        self.session = nil
        self.revision &+= 1
        self.log("session_finished session=\(sessionID)")
        self.wake()
    }

    func beginShutdown() {
        guard !self.isShuttingDown else { return }
        self.isShuttingDown = true
        self.session = nil
        self.revision &+= 1
        self.wake()
    }

    func shutdown() async {
        self.beginShutdown()
        await self.waitUntilSettled()
    }

    /// Also used by deterministic tests; no continuous observer or polling task.
    func waitUntilSettled() async {
        while let worker = self.worker {
            await worker.value
        }
    }

    private func wake() {
        guard self.worker == nil else { return }
        self.worker = Task { await self.reconcile() }
    }

    private func reconcile() async {
        while true {
            let observedRevision = self.revision
            if let session = self.session {
                if session.mayPause, self.attemptedSession != session.id {
                    self.attemptedSession = session.id
                    await self.pause(sessionID: session.id)
                }
            } else if let target = self.pausedTarget {
                await self.resume(target: target)
            }
            guard observedRevision != self.revision else {
                self.worker = nil
                return
            }
        }
    }

    private func canPause(_ sessionID: Int) -> Bool {
        self.session?.id == sessionID && self.session?.mayPause == true
    }

    private func pause(sessionID: Int) async {
        guard self.now() >= self.suspendedUntil else {
            self.log("pause_suppressed session=\(sessionID) reason=player_backoff")
            return
        }
        guard let before = await self.queryBeforePause(sessionID: sessionID) else { return }
        guard self.canPause(sessionID) else {
            self.log("pause_skipped session=\(sessionID) reason=stale_recording")
            return
        }
        if let owned = self.pausedTarget, owned.matches(before), before.isPlaying == false {
            self.log("pause_retained session=\(sessionID) reason=already_owned")
            return
        }
        // A player change or manual playback invalidates our previous ownership.
        self.pausedTarget = nil
        guard before.isPlaying == true else {
            self.log("pause_skipped session=\(sessionID) reason=not_known_playing")
            return
        }
        let result = await self.transport.send(.pause)
        self.logCommand(.pause, result: result, sessionID: sessionID)
        // Even a timed-out helper might have sent its command before exiting.
        // Observe state before deciding whether we own a pause to restore.
        if let paused = await self.verify(target: before, playing: false, context: "pause session=\(sessionID)") {
            self.pausedTarget = paused
            self.log("pause_verified session=\(sessionID)")
        } else {
            self.backOff(context: "pause session=\(sessionID)")
        }
    }

    private func queryBeforePause(sessionID: Int) async -> MediaPlaybackSnapshot? {
        for attempt in 1...3 {
            guard self.canPause(sessionID) else { return nil }
            if let snapshot = await self.query(context: "before_pause session=\(sessionID) attempt=\(attempt)") {
                return snapshot
            }
            guard self.canPause(sessionID) else { return nil }
            if attempt < 3 { await self.settle() }
        }
        return nil
    }

    private func resume(target: MediaPlaybackSnapshot) async {
        // Retry a command only after fresh metadata confirms the same paused item.
        // Retain ownership while a new recording may inherit the pause.
        for attempt in 1...2 {
            guard let before = await self.queryBeforeResume() else {
                guard self.session == nil else { return }
                // Missing metadata is not evidence that our confirmed pause ended.
                // Allow one more bounded read cycle; never issue Play without a
                // matching paused item. Keep ownership for the next session/quit
                // event if the outage outlasts this recovery window.
                if !self.isShuttingDown, attempt < 2 {
                    await self.settle()
                    continue
                }
                self.log("resume_deferred reason=unknown_player ownership=retained")
                return
            }
            guard self.session == nil else {
                self.log("resume_skipped reason=new_recording")
                return
            }
            guard target.matches(before), before.isPlaying == false else {
                self.pausedTarget = nil
                self.log("resume_skipped reason=player_item_or_state_changed")
                return
            }
            let result = await self.transport.send(.play)
            self.logCommand(.play, result: result, sessionID: nil)
            if await self.verify(target: before, playing: true, context: "resume") != nil {
                self.pausedTarget = nil
                self.log("resume_verified")
                return
            }
            guard self.session == nil else { return }
            // Quit is best-effort within the app's overall termination deadline.
            // Do not add another command cycle while shutdown is in progress.
            if self.isShuttingDown || attempt == 2 {
                self.pausedTarget = nil
                self.backOff(context: "resume")
                return
            }
        }
    }

    private func queryBeforeResume() async -> MediaPlaybackSnapshot? {
        // Retain confirmed ownership across brief metadata outages. Retry reads,
        // never playback commands; give up after three bounded helper calls.
        for attempt in 1...3 {
            guard self.session == nil else { return nil }
            if let snapshot = await self.query(context: "before_resume attempt=\(attempt)") {
                return snapshot
            }
            guard self.session == nil else { return nil }
            if self.isShuttingDown { return nil }
            if attempt < 3 { await self.settle() }
        }
        return nil
    }

    private func verify(
        target: MediaPlaybackSnapshot, playing: Bool, context: String
    ) async -> MediaPlaybackSnapshot? {
        // Read at most twice; never retry a playback command blindly. This checks
        // reported state, not rendered video: Netflix can disagree with its UI.
        for attempt in 1...2 {
            if attempt > 1, self.isShuttingDown { break }
            await self.settle()
            guard let observed = await self.query(context: "verify_\(context) attempt=\(attempt)") else {
                continue
            }
            guard target.matches(observed) else {
                self.log("verification_failed context=\(context) reason=player_or_item_changed")
                return nil
            }
            if observed.isPlaying == playing { return observed }
        }
        self.log("verification_failed context=\(context) reason=state_not_confirmed")
        return nil
    }

    private func query(context: String) async -> MediaPlaybackSnapshot? {
        let started = self.now()
        let result = await self.transport.query()
        let elapsed = Int((self.now() - started) * 1000)
        switch result {
        case let .snapshot(snapshot):
            self.log(
                "query context=\(context) elapsedMs=\(elapsed) " +
                    "bundle=\(snapshot.bundleIdentifier) pid=\(snapshot.processID) " +
                    "playing=\(snapshot.isPlaying.map(String.init) ?? "unknown") " +
                    "hasTitle=\(snapshot.title != nil)"
            )
            return snapshot
        case let .unavailable(reason):
            self.log("query_unavailable context=\(context) elapsedMs=\(elapsed) reason=\(reason)")
            return nil
        }
    }

    private func backOff(context: String) {
        // A finite cooldown limits damage from hotkey spam against an unresponsive
        // player. The next recording after the cooldown performs a fresh query.
        self.suspendedUntil = self.now() + 10
        self.log("commands_suspended context=\(context) seconds=10")
    }

    private func logCommand(
        _ command: MediaPlaybackCommand, result: MediaPlaybackCommandResult, sessionID: Int?
    ) {
        let status: String
        switch result {
        case .helperCompleted: status = "helper_completed_player_unconfirmed"
        case let .failed(reason): status = "failed:\(reason)"
        }
        self.log("command=\(command.rawValue) session=\(sessionID.map(String.init) ?? "none") result=\(status)")
    }

    private func log(_ message: String) {
        DebugLogger.shared.info("MEDIA_CONTROL \(message)", source: "MediaPlaybackService")
    }

    // MARK: - Duck path

    /// Snapshots the current system output volume and starts a background
    /// fade-down to `kDuckTargetVolume`. Returns the snapshot so the caller
    /// can hand it back to `restoreSystemVolume(previous:)` on stop.
    ///
    /// Returns `nil` if the volume couldn't be read, or if the user's
    /// volume is already at or below the duck target — in either case we
    /// don't touch the volume at all (and the matching restore becomes a
    /// no-op).
    func duckSystemVolume(targetVolume: Float? = nil) -> SystemVolumeSnapshot? {
        let duckTarget = targetVolume ?? Float(SettingsStore.shared.duckMediaVolumeLevel)
        guard let snapshot = SystemVolumeController.currentSnapshot() else {
            DebugLogger.shared.debug(
                "MediaPlaybackService: Couldn't read system volume, skipping duck",
                source: "MediaPlaybackService"
            )
            return nil
        }
        let previousScalar = snapshot.averageScalar
        guard previousScalar > duckTarget else {
            DebugLogger.shared.debug(
                "MediaPlaybackService: Volume \(String(format: "%.2f", previousScalar)) already ≤ duck target \(String(format: "%.2f", duckTarget)), skipping",
                source: "MediaPlaybackService"
            )
            return nil
        }

        // Snap the volume halfway down to the duck target SYNCHRONOUSLY before
        // starting the detached fade. This puts a clearly audible drop on the
        // user's ear within the round-trip time of one CoreAudio property
        // write (sub-millisecond), bypassing both Task.detached scheduling
        // latency and the fade ramp's first few steps where the per-step
        // volume change is too small to perceive. The detached fade then
        // smoothly lands the rest of the way to duckTarget.
        let immediateDrop = (previousScalar + duckTarget) / 2
        SystemVolumeController.setVolume(immediateDrop)

        DebugLogger.shared.info(
            "🔉 Snapped \(String(format: "%.2f", previousScalar)) → \(String(format: "%.2f", immediateDrop)), fading to \(String(format: "%.2f", duckTarget)) over \(kFadeDuration)s",
            source: "MediaPlaybackService"
        )
        self.startFade(from: immediateDrop, to: duckTarget, restoreSnapshot: nil)
        return snapshot
    }

    /// Fades the system output volume back up to the snapshot captured by
    /// `duckSystemVolume()`. Reads the live volume first so a mid-fade
    /// interruption (user released the hotkey before the duck-down had
    /// finished) restarts cleanly from wherever the volume actually is.
    /// Re-applies the snapshot exactly at the end of the ramp so per-channel
    /// detail (e.g. uneven L/R balance) comes back precisely rather than
    /// flattened to the fade scalar.
    func restoreSystemVolume(previous: SystemVolumeSnapshot?) {
        guard let previous else { return }
        let duckTarget = Float(SettingsStore.shared.duckMediaVolumeLevel)
        let start = SystemVolumeController.currentVolume() ?? duckTarget
        let target = previous.averageScalar
        DebugLogger.shared.info(
            "🔊 Fading system volume \(String(format: "%.2f", start)) → \(String(format: "%.2f", target)) over \(kFadeDuration)s",
            source: "MediaPlaybackService"
        )
        self.startFade(from: start, to: target, restoreSnapshot: previous)
    }

    /// Cancels any in-flight fade and starts a new one from `start` to
    /// `target` over `kFadeDuration`. Runs detached so the main actor isn't
    /// blocked between steps; CoreAudio property writes are thread-safe.
    ///
    /// - Parameter restoreSnapshot: If non-nil, this snapshot is applied
    ///   exactly at the end of the ramp instead of writing the scalar
    ///   `target`. Used by the fade-up so per-channel volume detail
    ///   (uneven L/R balance) is restored precisely. Pass `nil` for the
    ///   fade-down — there's nothing to preserve at the duck target.
    private func startFade(from start: Float, to target: Float, restoreSnapshot: SystemVolumeSnapshot?) {
        self.activeFadeTask?.cancel()

        let stepCount = kFadeSteps
        let stepDelay = kFadeDuration / Double(stepCount)
        let stepDelayNanos = UInt64(stepDelay * 1_000_000_000)
        let delta = (target - start) / Float(stepCount)

        self.activeFadeTask = Task.detached(priority: .userInitiated) {
            for step in 1...stepCount {
                if Task.isCancelled { return }
                let value = start + delta * Float(step)
                _ = SystemVolumeController.setVolume(value)
                if step < stepCount {
                    try? await Task.sleep(nanoseconds: stepDelayNanos)
                }
            }
            // Land exactly on the target if we weren't cancelled —
            // floating-point drift across the steps could otherwise leave
            // us a hair off (e.g. 0.0997 instead of 0.10). If the caller
            // asked for an exact snapshot restore, prefer that over the
            // scalar target so per-channel detail comes back intact.
            if !Task.isCancelled {
                if let restoreSnapshot {
                    _ = SystemVolumeController.restore(restoreSnapshot)
                } else {
                    _ = SystemVolumeController.setVolume(target)
                }
            }
        }
    }

    // MARK: - Unified restore

    /// Undoes whatever `MediaSessionAction` was taken at recording start.
    func restore(from action: MediaSessionAction) async {
        switch action {
        case .none:
            return
        case .paused:
            if let target = self.pausedTarget {
                await self.resume(target: target)
            }
        case .ducked(let previousVolume):
            self.restoreSystemVolume(previous: previousVolume)
        }
    }
}
