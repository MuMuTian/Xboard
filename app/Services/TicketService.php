<?php
namespace App\Services;


use App\Exceptions\ApiException;
use App\Jobs\SendEmailJob;
use App\Models\Ticket;
use App\Models\TicketMessage;
use App\Models\User;
use App\Utils\Helper;
use Illuminate\Http\UploadedFile;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\DB;
use App\Services\Plugin\HookManager;

class TicketService
{
    /** 附件存放目录（public 下，随 web 根静态可访问） */
    private const ATTACHMENT_DIR = 'uploads/ticket';

    /** 单条消息允许的最大附件数量 */
    public const ATTACHMENT_MAX_COUNT = 5;

    /** 合法附件相对路径的白名单格式，防止客户端注入任意 URL */
    private const ATTACHMENT_PATH_PATTERN = '#^uploads/ticket/\d{6}/[A-Za-z0-9]+\.(jpg|jpeg|png|gif|webp)$#i';

    public function reply($ticket, $message, $userId, array $attachments = [])
    {
        try {
            DB::beginTransaction();
            $ticketMessage = TicketMessage::create([
                'user_id' => $userId,
                'ticket_id' => $ticket->id,
                'message' => $message,
                'attachments' => $this->normalizeAttachments($attachments),
            ]);
            $isAdmin = $userId !== $ticket->user_id;
            $ticket->reply_status = $isAdmin
                ? Ticket::REPLY_STATUS_REPLIED
                : Ticket::REPLY_STATUS_WAITING;
            $ticket->last_reply_user_id = $userId;
            if (!$ticketMessage || !$ticket->save()) {
                throw new \Exception();
            }
            DB::commit();
            return $ticketMessage;
        } catch (\Exception $e) {
            DB::rollback();
            return false;
        }
    }

    public function replyByAdmin($ticketId, $message, $userId, array $attachments = []): void
    {
        $ticket = Ticket::where('id', $ticketId)->first();
        if (!$ticket) {
            throw new ApiException('工单不存在');
        }
        $ticketMessage = $this->reply($ticket, $message, $userId, $attachments);
        if (!$ticketMessage) {
            throw new ApiException('工单回复失败');
        }
        HookManager::call('ticket.reply.admin.after', [$ticket, $ticketMessage]);
        $this->sendEmailNotify($ticket, $ticketMessage);
    }

    public function createTicket($userId, $subject, $level, $message, array $attachments = [])
    {
        try {
            DB::beginTransaction();
            if (Ticket::where('status', 0)->where('user_id', $userId)->lockForUpdate()->first()) {
                DB::rollBack();
                throw new ApiException('存在未关闭的工单');
            }
            $ticket = Ticket::create([
                'user_id' => $userId,
                'subject' => $subject,
                'level' => $level,
                'reply_status' => Ticket::REPLY_STATUS_WAITING,
                'last_reply_user_id' => $userId,
            ]);
            if (!$ticket) {
                throw new ApiException('工单创建失败');
            }
            $ticketMessage = TicketMessage::create([
                'user_id' => $userId,
                'ticket_id' => $ticket->id,
                'message' => $message,
                'attachments' => $this->normalizeAttachments($attachments),
            ]);
            if (!$ticketMessage) {
                DB::rollBack();
                throw new ApiException('工单消息创建失败');
            }
            DB::commit();
            return $ticket;
        } catch (\Exception $e) {
            DB::rollBack();
            throw $e;
        }
    }

    // 半小时内不再重复通知
    private function sendEmailNotify(Ticket $ticket, TicketMessage $ticketMessage)
    {
        $user = User::find($ticket->user_id);
        $cacheKey = 'ticket_sendEmailNotify_' . $ticket->user_id;
        if (!Cache::get($cacheKey)) {
            Cache::put($cacheKey, 1, 1800);
            SendEmailJob::dispatch([
                'email' => $user->email,
                'subject' => '您在' . admin_setting('app_name', 'XBoard') . '的工单得到了回复',
                'template_name' => 'notify',
                'template_value' => [
                    'name' => admin_setting('app_name', 'XBoard'),
                    'url' => admin_setting('app_url'),
                    'content' => "主题：{$ticket->subject}\r\n回复内容：{$ticketMessage->message}"
                ]
            ]);
        }
    }

    /**
     * 保存一张工单附件图片到 public 目录，返回其相对路径与完整 URL。
     * 文件本身的类型/大小校验由调用方（FormRequest）负责。
     */
    public function storeAttachment(UploadedFile $file): array
    {
        $ext = strtolower($file->getClientOriginalExtension() ?: ($file->extension() ?: 'png'));
        $dir = self::ATTACHMENT_DIR . '/' . date('Ym');
        $absDir = public_path($dir);
        if (!is_dir($absDir)) {
            @mkdir($absDir, 0755, true);
        }
        $filename = Helper::guid() . '.' . $ext;
        $file->move($absDir, $filename);

        $relativePath = $dir . '/' . $filename;
        return [
            'path' => $relativePath,
            'url' => asset($relativePath),
        ];
    }

    /**
     * 过滤客户端提交的附件列表：只接受本站上传目录下、符合命名规则的相对路径，
     * 去重并限制数量，避免任意 URL / 路径穿越注入。
     */
    public function normalizeAttachments($attachments): ?array
    {
        if (!is_array($attachments)) {
            return null;
        }

        $result = [];
        foreach ($attachments as $item) {
            if (!is_string($item)) {
                continue;
            }
            // 兼容前端回传完整 URL 的情况，统一还原为相对路径
            $path = ltrim(parse_url($item, PHP_URL_PATH) ?: $item, '/');
            if (str_contains($path, '..')) {
                continue;
            }
            if (preg_match(self::ATTACHMENT_PATH_PATTERN, $path)) {
                $result[$path] = true;
            }
            if (count($result) >= self::ATTACHMENT_MAX_COUNT) {
                break;
            }
        }

        return $result === [] ? null : array_keys($result);
    }
}
