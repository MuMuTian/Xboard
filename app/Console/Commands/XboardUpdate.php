<?php

namespace App\Console\Commands;

use App\Services\ThemeService;
use App\Services\UpdateService;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Artisan;
use App\Services\Plugin\PluginManager;

class XboardUpdate extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'xboard:update';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'xboard 更新';

    /**
     * Create a new command instance.
     *
     * @return void
     */
    public function __construct()
    {
        parent::__construct();
    }

    /**
     * Execute the console command.
     *
     * @return mixed
     */
    public function handle()
    {
        $this->info('正在导入数据库请稍等...');
        Artisan::call("migrate", ['--force' => true]);
        $this->info(Artisan::output());
        $this->info('正在检查并安装默认插件...');
        PluginManager::installDefaultPlugins();
        $this->info('默认插件检查完成');
        $updateService = new UpdateService();
        $updateService->updateVersionCache();
        $themeService = app(ThemeService::class);
        $themeService->refreshCurrentTheme();
        if (config('queue.default') === 'sync') {
            $this->info('horizon:terminate skipped (sync queue, no workers to terminate).');
        } elseif ($this->horizonDisabledHere()) {
            // 拆分部署时本容器不跑 horizon。horizon:terminate 会按 Redis 中记录的
            // master PID 发送 SIGTERM，而各容器 PID 命名空间互相独立；若容器间
            // 共用 hostname（例如 network_mode: host），该 PID 会命中本容器内的
            // 无关进程，把自己杀掉（退出码 143）并陷入重启循环。
            $this->info('horizon:terminate skipped (horizon not enabled in this container).');
        } else {
            try {
                Artisan::call('horizon:terminate');
            } catch (\Throwable $e) {
                $this->warn('horizon:terminate skipped: ' . $e->getMessage());
            }
        }
        $this->info('更新完毕，队列服务已重启，你无需进行任何操作。');
    }

    /**
     * 本容器是否已显式关闭 horizon（拆分部署中的 web / ws-server 容器）。
     * 未设置该变量时返回 false，保持单容器部署的原有行为。
     */
    private function horizonDisabledHere(): bool
    {
        $flag = getenv('ENABLE_HORIZON');
        if ($flag === false || $flag === '') {
            return false;
        }
        return in_array(strtolower(trim($flag)), ['false', '0', 'no', 'off'], true);
    }
}
