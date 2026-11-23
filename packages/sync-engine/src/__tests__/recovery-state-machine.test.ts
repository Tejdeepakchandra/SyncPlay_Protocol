import { describe, it, expect, vi } from 'vitest';
import {
  RecoveryStateMachine,
  type RecoveryState,
  type RecoverySubstate,
} from '../recovery/recovery-state-machine.js';
import { SyncEngineError } from '../errors.js';

describe('RecoveryStateMachine (Phase 7.5 M4.3a)', () => {
  it('initializes in ACTIVE / IDLE by default', () => {
    const sm = new RecoveryStateMachine();
    expect(sm.state).toBe('ACTIVE');
    expect(sm.substate).toBe('IDLE');
    expect(sm.isDisposed).toBe(false);
  });

  it('allows custom initial state and substate', () => {
    const sm = new RecoveryStateMachine('RECOVERING', 'TRANSFERRING');
    expect(sm.state).toBe('RECOVERING');
    expect(sm.substate).toBe('TRANSFERRING');
  });

  describe('Valid Transitions Graph', () => {
    it('executes valid transitions from ACTIVE', () => {
      // ACTIVE -> RECOVERING
      const sm1 = new RecoveryStateMachine('ACTIVE');
      expect(sm1.canTransition('RECOVERING')).toBe(true);
      sm1.transition('RECOVERING', 'Offer accepted', 'TRANSFERRING');
      expect(sm1.state).toBe('RECOVERING');
      expect(sm1.substate).toBe('TRANSFERRING');

      // ACTIVE -> DEGRADED
      const sm2 = new RecoveryStateMachine('ACTIVE');
      expect(sm2.canTransition('DEGRADED')).toBe(true);
      sm2.transition('DEGRADED', 'Unrecoverable divergence');
      expect(sm2.state).toBe('DEGRADED');
      expect(sm2.substate).toBe('IDLE');
    });

    it('executes valid transitions from RECOVERING', () => {
      // RECOVERING -> RECONCILING
      const sm1 = new RecoveryStateMachine('RECOVERING');
      expect(sm1.canTransition('RECONCILING')).toBe(true);
      sm1.transition('RECONCILING', 'Snapshot base committed', 'DRAINING_QUARANTINE');
      expect(sm1.state).toBe('RECONCILING');
      expect(sm1.substate).toBe('DRAINING_QUARANTINE');

      // RECOVERING -> DEGRADED
      const sm2 = new RecoveryStateMachine('RECOVERING');
      expect(sm2.canTransition('DEGRADED')).toBe(true);
      sm2.transition('DEGRADED', 'Candidates exhausted');
      expect(sm2.state).toBe('DEGRADED');

      // RECOVERING -> ACTIVE (Clean abort before commit)
      const sm3 = new RecoveryStateMachine('RECOVERING');
      expect(sm3.canTransition('ACTIVE')).toBe(true);
      sm3.transition('ACTIVE', 'Clean abort before commit');
      expect(sm3.state).toBe('ACTIVE');
    });

    it('executes valid transitions from RECONCILING', () => {
      // RECONCILING -> ACTIVE
      const sm1 = new RecoveryStateMachine('RECONCILING');
      expect(sm1.canTransition('ACTIVE')).toBe(true);
      sm1.transition('ACTIVE', 'Drain complete, handoff successful');
      expect(sm1.state).toBe('ACTIVE');

      // RECONCILING -> DEGRADED
      const sm2 = new RecoveryStateMachine('RECONCILING');
      expect(sm2.canTransition('DEGRADED')).toBe(true);
      sm2.transition('DEGRADED', 'Quarantine overflow in drain');
      expect(sm2.state).toBe('DEGRADED');
    });

    it('executes valid transitions from DEGRADED', () => {
      // DEGRADED -> RECOVERING
      const sm = new RecoveryStateMachine('DEGRADED');
      expect(sm.canTransition('RECOVERING')).toBe(true);
      sm.transition('RECOVERING', 'New dominating offer received', 'AWAITING_OFFER');
      expect(sm.state).toBe('RECOVERING');
      expect(sm.substate).toBe('AWAITING_OFFER');
    });
  });

  describe('Invalid Transitions Prohibition', () => {
    it('rejects ACTIVE -> RECONCILING without passing through RECOVERING', () => {
      const sm = new RecoveryStateMachine('ACTIVE');
      expect(sm.canTransition('RECONCILING')).toBe(false);
      expect(() => sm.transition('RECONCILING', 'Illegal leap')).toThrow(SyncEngineError);
      expect(() => sm.transition('RECONCILING', 'Illegal leap')).toThrow(
        /Illegal recovery transition from ACTIVE to RECONCILING/,
      );
      expect(sm.state).toBe('ACTIVE'); // State unmutated
    });

    it('rejects RECONCILING -> RECOVERING (Post-commit rollback prohibited)', () => {
      const sm = new RecoveryStateMachine('RECONCILING');
      expect(sm.canTransition('RECOVERING')).toBe(false);
      expect(() => sm.transition('RECOVERING', 'Attempt rollback')).toThrow(SyncEngineError);
      expect(sm.state).toBe('RECONCILING'); // State unmutated
    });

    it('rejects DEGRADED -> ACTIVE (Cannot return to ACTIVE directly from DEGRADED)', () => {
      const sm = new RecoveryStateMachine('DEGRADED');
      expect(sm.canTransition('ACTIVE')).toBe(false);
      expect(() => sm.transition('ACTIVE', 'Direct reactivation')).toThrow(SyncEngineError);
      expect(sm.state).toBe('DEGRADED'); // State unmutated
    });

    it('rejects DEGRADED -> RECONCILING directly', () => {
      const sm = new RecoveryStateMachine('DEGRADED');
      expect(sm.canTransition('RECONCILING')).toBe(false);
      expect(() => sm.transition('RECONCILING', 'Illegal reconcile')).toThrow(SyncEngineError);
      expect(sm.state).toBe('DEGRADED');
    });

    it('rejects redundant self-transitions (e.g. ACTIVE -> ACTIVE)', () => {
      const sm = new RecoveryStateMachine('ACTIVE');
      expect(sm.canTransition('ACTIVE')).toBe(false);
      expect(() => sm.transition('ACTIVE', 'Noop')).toThrow(SyncEngineError);
      expect(sm.state).toBe('ACTIVE');
    });
  });

  describe('Granular Substate Management', () => {
    it('updates substate within valid state', () => {
      const sm = new RecoveryStateMachine('RECOVERING', 'AWAITING_OFFER');
      const listener = vi.fn();
      sm.onSubstateChange(listener);

      sm.setSubstate('TRANSFERRING');
      expect(sm.substate).toBe('TRANSFERRING');
      expect(listener).toHaveBeenCalledWith('TRANSFERRING', 'AWAITING_OFFER');

      sm.setSubstate('VALIDATING');
      expect(sm.substate).toBe('VALIDATING');
      expect(listener).toHaveBeenCalledWith('VALIDATING', 'TRANSFERRING');
    });

    it('ignores redundant substate updates without notifying listeners', () => {
      const sm = new RecoveryStateMachine('RECOVERING', 'TRANSFERRING');
      const listener = vi.fn();
      sm.onSubstateChange(listener);

      sm.setSubstate('TRANSFERRING');
      expect(listener).not.toHaveBeenCalled();
    });
  });

  describe('Observer Notifications & Exception Isolation', () => {
    it('notifies multiple state listeners in registration order', () => {
      const sm = new RecoveryStateMachine('ACTIVE');
      const order: number[] = [];

      sm.onStateChange((newState, oldState, reason) => {
        order.push(1);
        expect(newState).toBe('RECOVERING');
        expect(oldState).toBe('ACTIVE');
        expect(reason).toBe('Network drop');
      });

      sm.onStateChange(() => {
        order.push(2);
      });

      sm.transition('RECOVERING', 'Network drop');
      expect(order).toEqual([1, 2]);
    });

    it('isolates listener exceptions so other listeners continue and state is safe', () => {
      const sm = new RecoveryStateMachine('ACTIVE');
      const listenerAfterThrow = vi.fn();

      sm.onStateChange(() => {
        throw new Error('Subscriber crash in UI callback');
      });

      sm.onStateChange(listenerAfterThrow);

      expect(() => sm.transition('RECOVERING', 'Normal drop')).not.toThrow();
      expect(sm.state).toBe('RECOVERING');
      expect(listenerAfterThrow).toHaveBeenCalledWith('RECOVERING', 'ACTIVE', 'Normal drop');
    });

    it('supports unsubscribing listeners', () => {
      const sm = new RecoveryStateMachine('ACTIVE');
      const listener = vi.fn();
      const unsub = sm.onStateChange(listener);

      unsub();
      sm.transition('RECOVERING', 'Test');
      expect(listener).not.toHaveBeenCalled();
    });
  });

  describe('Disposal and Idempotency', () => {
    it('disposes cleanly and idempotently', () => {
      const sm = new RecoveryStateMachine('ACTIVE');
      expect(sm.isDisposed).toBe(false);

      sm.dispose();
      expect(sm.isDisposed).toBe(true);

      // Second dispose call should not throw
      expect(() => sm.dispose()).not.toThrow();
      expect(sm.isDisposed).toBe(true);
    });

    it('rejects transitions and substate changes after disposal', () => {
      const sm = new RecoveryStateMachine('ACTIVE');
      sm.dispose();

      expect(sm.canTransition('RECOVERING')).toBe(false);
      expect(() => sm.transition('RECOVERING', 'Late call')).toThrow(SyncEngineError);
      expect(() => sm.transition('RECOVERING', 'Late call')).toThrow(/disposed/);

      expect(() => sm.setSubstate('TRANSFERRING')).toThrow(SyncEngineError);
    });

    it('ignores unsubscribe calls after disposal', () => {
      const sm = new RecoveryStateMachine('ACTIVE');
      const unsub = sm.onStateChange(() => {});
      sm.dispose();
      expect(() => unsub()).not.toThrow();
    });
  });
});
