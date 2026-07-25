<?php

namespace App\Jobs;

use App\Models\StatUser;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;

class StatUserJob implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    protected array $data;
    protected array $server;
    protected string $protocol;
    protected string $recordType;

    public $tries = 3;
    public $timeout = 60;
    public $maxExceptions = 3;

    public function backoff(): array
    {
        return [1, 5, 10];
    }

    public function __construct(array $server, array $data, string $protocol, string $recordType = 'd')
    {
        $this->onQueue('stat');
        $this->data = $data;
        $this->server = $server;
        $this->protocol = $protocol;
        $this->recordType = $recordType;
    }

    public function handle(): void
    {
        if (empty($this->data)) {
            return;
        }

        $recordAt = $this->recordType === 'm'
            ? strtotime(date('Y-m-01'))
            : strtotime(date('Y-m-d'));

        $rate = $this->server['rate'];
        $now  = time();

        // 一次性把 chunk 里所有用户拼成 upsert 多行 SQL
        $rows = [];
        foreach ($this->data as $uid => $v) {
            $rows[] = [
                'user_id'     => (int) $uid,
                'server_rate' => $rate,
                'record_at'   => $recordAt,
                'record_type' => $this->recordType,
                'u'           => intval($v[0] * $rate),
                'd'           => intval($v[1] * $rate),
                'created_at'  => $now,
                'updated_at'  => $now,
            ];
        }

        try {
            StatUser::upsert(
                $rows,
                ['user_id', 'server_rate', 'record_at', 'record_type'],
                [
                    'u' => DB::raw('u + VALUES(u)'),
                    'd' => DB::raw('d + VALUES(d)'),
                    'updated_at' => $now,
                ]
            );
        } catch (\Throwable $e) {
            Log::error('StatUserJob batch upsert failed', [
                'server_id' => $this->server['id'] ?? null,
                'count'     => count($rows),
                'message'   => $e->getMessage(),
            ]);
            throw $e;
        }
    }
}
