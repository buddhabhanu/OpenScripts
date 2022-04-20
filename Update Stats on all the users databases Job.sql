USE [msdb]
GO

/****** Object:  Job [Update Stats Job on User DBs]    Script Date: 4/20/2022 3:14:08 PM ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [[Uncategorized (Local)]]    Script Date: 4/20/2022 3:14:08 PM ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'Update Stats Job on User DBs', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'No description available.', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'sa', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Update Stats on Read write DBs]    Script Date: 4/20/2022 3:14:08 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Update Stats on Read write DBs', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'
Set quoted_identifier off
use master
go


DECLARE @fillfactor varchar(5)
DECLARE @tablename varchar(300)
DECLARE @tablename_header varchar(300)
DECLARE @dataname varchar(300)
DECLARE @dataname_header varchar(300)
DECLARE datanames_cursor CURSOR FOR SELECT name FROM sys.databases
        WHERE  user_access_desc=''MULTI_USER'' and database_id>2  -- name not in (''master'', ''pubs'', ''tempdb'', ''model'', ''northwind'')
/* Variable Initialization */select @fillfactor = "0"-- Set Fill factor here
-- Note "0" will use original fillfactor.
/* End Variable Initialization */OPEN datanames_cursor

  FETCH NEXT FROM datanames_cursor INTO @dataname

  WHILE (@@fetch_status <> -1)
    BEGIN
      IF (@@fetch_status = -2)
        BEGIN
FETCH NEXT FROM datanames_cursor INTO @dataname
          CONTINUE
        END
SELECT @dataname_header = "Database " + RTRIM(UPPER(@dataname))
      PRINT " "
PRINT @dataname_header
      PRINT " "
EXEC ("USE " + @dataname + " DECLARE tnames_cursor CURSOR FOR SELECT name from sysobjects where type = ''U'' order by name ")
Select @dataname_header = RTRIM(UPPER(@dataname))
Exec ("Use " + @dataname) 
OPEN tnames_cursor
FETCH NEXT FROM tnames_cursor INTO @tablename
WHILE (@@fetch_status <> -1)
        BEGIN
          IF (@@fetch_status = -2)            
BEGIN
              FETCH NEXT FROM tnames_cursor INTO @tablename
              CONTINUE
            END
    SELECT @tablename_header = "  UPDATE STATISTICS [" + RTRIM(UPPER(@tablename)) + "] WITH SAMPLE 20 PERCENT"

          PRINT @tablename_header
		  
--EXEC ("USE " + @dataname + " DBCC DBREINDEX (" + @tablename + "," + "''''" + "," + @fillfactor + ")")
EXEC ("USE " + @dataname + " UPDATE STATISTICS [" + @tablename + "] WITH SAMPLE 20 PERCENT")
FETCH NEXT FROM tnames_cursor INTO @tablename
        END
DEALLOCATE tnames_cursor

      FETCH NEXT FROM datanames_cursor INTO @dataname
      END



DEALLOCATE datanames_cursor
PRINT ""
PRINT " "
PRINT "Update Stats Completed on All Read write User Databases"', 
		@database_name=N'master', 
		@output_file_name=N'E:\Data\Update_Stats_tables_log.txt', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO


