<?php


namespace App\Jobs;

use App\Models\Server;
use App\Models\StatServer;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Carbon;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;

class StatServerJob implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    protected array $data;
    protected array $server;
    protected string $protocol;
    protected string $recordType;

    // 统计写入同样是「u += delta」的非幂等累加（upsert 冲突时累加）。
    // 若 worker 在 upsert 提交后、ack 之前被 timeout/OOM 杀掉，重试会把同一批
    // 增量再加一遍，最多放大 3 倍。与 TrafficFetchJob 保持一致固定 tries=1：
    // 宁可漏记一个 chunk（轻微少报），也不能重复累加导致统计虚高。
    public $tries = 1;
    public $timeout = 60;
    public $maxExceptions = 1;

    /**
     * Create a new job instance.
     */
    public function __construct(array $server, array $data, $protocol, string $recordType = 'd')
    {
        $this->onQueue('stat');
        $this->data = $data;
        $this->server = $server;
        $this->protocol = $protocol;
        $this->recordType = $recordType;
    }

    public function handle(): void
    {
        $recordAt = $this->recordType === 'm'
            ? strtotime(date('Y-m-01'))
            : strtotime(date('Y-m-d'));

        $u = $d = 0;
        foreach ($this->data as $traffic) {
            $u += $traffic[0];
            $d += $traffic[1];
        }

        try {
            $this->processServerStat($u, $d, $recordAt);
            $this->updateServerTraffic($u, $d);
        } catch (\Exception $e) {
            Log::error('StatServerJob failed for server ' . $this->server['id'] . ': ' . $e->getMessage());
            throw $e;
        }
    }

    protected function updateServerTraffic(int $u, int $d): void
    {
        DB::table('v2_server')
            ->where('id', $this->server['id'])
            ->incrementEach(
                ['u' => $u, 'd' => $d],
                ['updated_at' => Carbon::now()]
            );
    }

    protected function processServerStat(int $u, int $d, int $recordAt): void
    {
        $driver = config('database.default');
        if ($driver === 'sqlite') {
            $this->processServerStatForSqlite($u, $d, $recordAt);
        } elseif ($driver === 'pgsql') {
            $this->processServerStatForPostgres($u, $d, $recordAt);
        } else {
            $this->processServerStatForOtherDatabases($u, $d, $recordAt);
        }
    }

    protected function processServerStatForSqlite(int $u, int $d, int $recordAt): void
    {
        DB::transaction(function () use ($u, $d, $recordAt) {
            $existingRecord = StatServer::where([
                'record_at' => $recordAt,
                'server_id' => $this->server['id'],
                'server_type' => $this->protocol,
                'record_type' => $this->recordType,
            ])->first();

            if ($existingRecord) {
                $existingRecord->update([
                    'u' => $existingRecord->u + $u,
                    'd' => $existingRecord->d + $d,
                    'updated_at' => time(),
                ]);
            } else {
                StatServer::create([
                    'record_at' => $recordAt,
                    'server_id' => $this->server['id'],
                    'server_type' => $this->protocol,
                    'record_type' => $this->recordType,
                    'u' => $u,
                    'd' => $d,
                    'created_at' => time(),
                    'updated_at' => time(),
                ]);
            }
        }, 3);
    }

    protected function processServerStatForOtherDatabases(int $u, int $d, int $recordAt): void
    {
        StatServer::upsert(
            [
                'record_at' => $recordAt,
                'server_id' => $this->server['id'],
                'server_type' => $this->protocol,
                'record_type' => $this->recordType,
                'u' => $u,
                'd' => $d,
                'created_at' => time(),
                'updated_at' => time(),
            ],
            ['server_id', 'server_type', 'record_at', 'record_type'],
            [
                'u' => DB::raw("u + VALUES(u)"),
                'd' => DB::raw("d + VALUES(d)"),
                'updated_at' => time(),
            ]
        );
    }

    /**
     * PostgreSQL upsert with arithmetic increments using ON CONFLICT ... DO UPDATE
     */
    protected function processServerStatForPostgres(int $u, int $d, int $recordAt): void
    {
        $table = (new StatServer())->getTable();
        $now = time();

        // Use parameter binding to avoid SQL injection and keep maintainability
        $sql = "INSERT INTO {$table} (record_at, server_id, server_type, record_type, u, d, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (server_id, server_type, record_at)
                DO UPDATE SET
                    u = {$table}.u + EXCLUDED.u,
                    d = {$table}.d + EXCLUDED.d,
                    updated_at = EXCLUDED.updated_at";

        DB::statement($sql, [
            $recordAt,
            $this->server['id'],
            $this->protocol,
            $this->recordType,
            $u,
            $d,
            $now,
            $now,
        ]);
    }
}
