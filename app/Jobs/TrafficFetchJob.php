<?php

namespace App\Jobs;

use App\Models\User;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Redis;

class TrafficFetchJob implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    protected $data;
    protected $server;
    protected $protocol;
    protected $timestamp;

    public $tries = 2;
    public $timeout = 60;

    public function __construct(array $server, array $data, $protocol, int $timestamp)
    {
        $this->onQueue('traffic_fetch');
        $this->server = $server;
        $this->data = $data;
        $this->protocol = $protocol;
        $this->timestamp = $timestamp;
    }

    public function handle(): void
    {
        if (empty($this->data)) {
            return;
        }

        $rate = (float) ($this->server['rate'] ?? 1);
        $now  = time();
        $uids = array_map('intval', array_keys($this->data));

        // 拼一条 CASE WHEN 批量 UPDATE：一次 round-trip 完成 chunk 所有用户
        $caseU = '';
        $caseD = '';
        foreach ($this->data as $uid => $v) {
            $uid = (int) $uid;
            $u   = (int) ($v[0] * $rate);
            $d   = (int) ($v[1] * $rate);
            $caseU .= " WHEN id = {$uid} THEN u + {$u}";
            $caseD .= " WHEN id = {$uid} THEN d + {$d}";
        }
        $idsCsv = implode(',', $uids);

        try {
            DB::statement(
                "UPDATE v2_user
                 SET u = CASE{$caseU} ELSE u END,
                     d = CASE{$caseD} ELSE d END,
                     t = {$now}
                 WHERE id IN ({$idsCsv})"
            );
        } catch (\Throwable $e) {
            Log::error('TrafficFetchJob batch update failed', [
                'server_id' => $this->server['id'] ?? null,
                'uid_count' => count($uids),
                'message'   => $e->getMessage(),
            ]);
            throw $e;
        }

        if (!empty($uids)) {
            Redis::sadd('traffic:pending_check', ...$uids);
        }
    }
}
