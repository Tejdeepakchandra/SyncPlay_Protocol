/**
 * SyncPlay Controls.
 *
 * Implements the developer-facing SyncPlayControls interface.
 * Delegates intent execution to the ControlEventService.
 */

import type {
  SyncPlayControls,
  CommandResult,
  AnyControlSyncEvent,
  ControlIntent,
} from './types.js';

export interface IntentExecutor {
  executeIntent(intent: ControlIntent): Promise<CommandResult>;
  executeRetry(event: AnyControlSyncEvent): Promise<CommandResult>;
}

export class ControlsImpl implements SyncPlayControls {
  constructor(private readonly executor: IntentExecutor) {}

  public play(targetPositionMs?: number): Promise<CommandResult> {
    return this.executor.executeIntent({ type: 'PLAY', targetPositionMs });
  }

  public pause(observedPositionMs?: number): Promise<CommandResult> {
    return this.executor.executeIntent({ type: 'PAUSE', observedPositionMs });
  }

  public seek(targetPositionMs: number): Promise<CommandResult> {
    return this.executor.executeIntent({ type: 'SEEK', targetPositionMs });
  }

  public setPlaybackRate(playbackRate: number): Promise<CommandResult> {
    return this.executor.executeIntent({ type: 'RATE_CHANGE', playbackRate });
  }

  public retry(event: AnyControlSyncEvent): Promise<CommandResult> {
    return this.executor.executeRetry(event);
  }
}
