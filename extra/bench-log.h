#pragma once

void SetLog(const char* full_path);
void FlushLog();
void LOG(const char* fmt, ...);
void StartBulkLog();
void EndBulkLog();