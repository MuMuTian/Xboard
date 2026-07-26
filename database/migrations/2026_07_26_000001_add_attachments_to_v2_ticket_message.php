<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration {
    public function up(): void
    {
        Schema::table('v2_ticket_message', function (Blueprint $table) {
            if (!Schema::hasColumn('v2_ticket_message', 'attachments')) {
                // 工单消息附件（图片）：存放相对路径数组，如 ["uploads/ticket/202607/xxxx.png"]
                $table->json('attachments')->nullable()->after('message');
            }
        });
    }

    public function down(): void
    {
        Schema::table('v2_ticket_message', function (Blueprint $table) {
            if (Schema::hasColumn('v2_ticket_message', 'attachments')) {
                $table->dropColumn('attachments');
            }
        });
    }
};
