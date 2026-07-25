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

        // 一次性把 chunk 里所有用户拼成批量写入的多行数据
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
            // 不同数据库的 upsert 增量语法不同：MySQL 用 VALUES()，
            // PostgreSQL 用 EXCLUDED，SQLite 建表时未创建唯一索引，需手动处理。
            match (DB::connection()->getDriverName()) {
                'pgsql'  => $this->upsertPostgres($rows, $now),
                'sqlite' => $this->upsertSqlite($rows, $now),
                default  => $this->upsertMysql($rows, $now),
            };
        } catch (\Throwable $e) {
            Log::error('StatUserJob batch upsert failed', [
                'server_id' => $this->server['id'] ?? null,
                'count'     => count($rows),
                'message'   => $e->getMessage(),
            ]);
            throw $e;
        }
    }

    /**
     * MySQL / MariaDB：单条多行 upsert，冲突时累加 u/d。
     */
    protected function upsertMysql(array $rows, int $now): void
    {
        StatUser::upsert(
            $rows,
            ['user_id', 'server_rate', 'record_at', 'record_type'],
            [
                'u' => DB::raw('u + VALUES(u)'),
                'd' => DB::raw('d + VALUES(d)'),
                'updated_at' => $now,
            ]
        );
    }

    /**
     * PostgreSQL：单条多行 INSERT ... ON CONFLICT DO UPDATE，
     * 冲突目标匹配唯一索引 (server_rate, user_id, record_at)。
     */
    protected function upsertPostgres(array $rows, int $now): void
    {
        $table = (new StatUser())->getTable();

        $placeholders = [];
        $bindings = [];
        foreach ($rows as $r) {
            $placeholders[] = '(?, ?, ?, ?, ?, ?, ?, ?)';
            array_push(
                $bindings,
                $r['user_id'],
                $r['server_rate'],
                $r['record_at'],
                $r['record_type'],
                $r['u'],
                $r['d'],
                $r['created_at'],
                $r['updated_at'],
            );
        }
        $values = implode(', ', $placeholders);

        $sql = "INSERT INTO {$table}
                    (user_id, server_rate, record_at, record_type, u, d, created_at, updated_at)
                VALUES {$values}
                ON CONFLICT (server_rate, user_id, record_at)
                DO UPDATE SET
                    u = {$table}.u + EXCLUDED.u,
                    d = {$table}.d + EXCLUDED.d,
                    updated_at = EXCLUDED.updated_at";

        DB::statement($sql, $bindings);
    }

    /**
     * SQLite：建表迁移未创建唯一索引，upsert 无冲突目标可用，
     * 因此在单个事务内手动 SELECT + 累加更新 / 插入。
     */
    protected function upsertSqlite(array $rows, int $now): void
    {
        DB::transaction(function () use ($rows, $now) {
            foreach ($rows as $r) {
                $existing = StatUser::where([
                    'user_id'     => $r['user_id'],
                    'server_rate' => $r['server_rate'],
                    'record_at'   => $r['record_at'],
                    'record_type' => $r['record_type'],
                ])->first();

                if ($existing) {
                    $existing->update([
                        'u' => $existing->u + $r['u'],
                        'd' => $existing->d + $r['d'],
                        'updated_at' => $now,
                    ]);
                } else {
                    StatUser::create($r);
                }
            }
        }, 3);
    }
}
