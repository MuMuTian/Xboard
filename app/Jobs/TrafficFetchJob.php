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

    // 流量记账是「u += delta」的非幂等累加：只要同一批数据被执行两次就会翻倍计费。
    // 因此固定 tries=1——宁可在极少数失败时漏记一个 chunk 的增量（轻微欠费），
    // 也绝不能因重试而重复计入，导致用户流量被多算、过快跑满。
    public $tries = 1;
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

        // pending_check 仅用于后续二次校验，属于尽力而为的操作。
        // 若在批量 UPDATE 成功后此处抛异常，因 tries=2 会触发整个 job 重试，
        // 导致 u/d 被重复累加。故单独捕获，失败只记录、不让 job 失败。
        if (!empty($uids)) {
            try {
                Redis::sadd('traffic:pending_check', ...$uids);
            } catch (\Throwable $e) {
                Log::warning('TrafficFetchJob pending_check enqueue failed', [
                    'server_id' => $this->server['id'] ?? null,
                    'uid_count' => count($uids),
                    'message'   => $e->getMessage(),
                ]);
            }
        }
    }
}
