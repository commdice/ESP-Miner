#ifndef STRATUM_TASK_H_
#define STRATUM_TASK_H_

#include "asic_job.h"
#include <stdint.h>
#include <stdbool.h>

typedef struct GlobalState GlobalState;

// Manages network connectivity, pool failover/heartbeat, and dispatches to V1/V2 protocol drivers.
void stratum_task(void *pvParameters);

// Request the active stratum client to reconnect (e.g. on primary pool recovery or pool settings change).
void stratum_request_reconnect(void);

// Returns true if a reconnect request is pending.
bool stratum_reconnect_requested(void);

// Probe a pool to check if it is reachable without disrupting active mining.
bool stratum_probe_pool(GlobalState *gs, uint16_t pool_idx);

// Promptly trigger a heartbeat check of the primary pool (e.g. when primary config changes while on fallback).
void stratum_trigger_heartbeat_check(void);

// Notify stratum task that a specific pool configuration was modified in NVS.
void stratum_notify_pool_modified(GlobalState *gs, uint16_t pool_idx);

// Notify stratum task that primary/secondary pool index or fallback toggle was changed.
void stratum_notify_pool_selection_changed(GlobalState *gs);

typedef struct {
    int32_t last_work_age_s;    // seconds since the active pool sent new work, -1 when not connected
    int32_t unanswered_age_s;   // seconds the oldest unanswered share has been waiting, 0 when none
    uint32_t reject_streak;     // consecutive rejected shares without an accept
    uint32_t actions;           // health-triggered failovers/reconnects since boot
    const char *last_reason;    // reason for the last health action, "" when none
    int32_t last_action_age_s;  // seconds since the last health action, -1 when none
    int32_t primary_holddown_s; // seconds before the primary pool may be probed again after a health failover
} pool_health_info_t;

// Snapshot of the active pool health tracked by the heartbeat task.
void stratum_get_pool_health(pool_health_info_t *out);

// Submit a found share to the active pool (dispatches to SV1 or SV2).
int stratum_submit_share(GlobalState *GLOBAL_STATE, const asic_job_t *active_job,
                         uint32_t nonce, uint32_t rolled_version, uint64_t *sent_time_us);

#endif /* STRATUM_TASK_H_ */
