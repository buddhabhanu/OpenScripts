USE [msdb]
GO

/****** Object:  Job [Index Defrag Job on User Dbs]    Script Date: 4/20/2022 3:14:36 PM ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [[Uncategorized (Local)]]    Script Date: 4/20/2022 3:14:36 PM ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'Index Defrag Job on User Dbs', 
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
/****** Object:  Step [Index Defrag on DB1]    Script Date: 4/20/2022 3:14:36 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Index Defrag on DB1', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'-- define index defrag rate for this run
DECLARE @fragrate float
SET @fragrate = 30.0 -- set to do something if defrag rate HIGH

DECLARE @rbrirate float
SET @rbrirate = 30.0 -- set to rebuild if defrag rate EXTREME

-- define table holding index information
DECLARE @indexinfo TABLE(objectid int, indexid int, partitionnum bigint, frag float)

DECLARE @objectid int
DECLARE @indexid int
DECLARE @partitioncount bigint
DECLARE @schemaname nvarchar(130) 
DECLARE @objectname nvarchar(130) 
DECLARE @indexname nvarchar(130) 
DECLARE @partitionnum bigint
DECLARE @partitions bigint
DECLARE @frag float
DECLARE @command nvarchar(4000) 

-- Conditionally select tables and indexes from the sys.dm_db_index_physical_stats function 
INSERT INTO @indexinfo
SELECT
    object_id AS objectid,
    index_id AS indexid,
    partition_number AS partitionnum,
    avg_fragmentation_in_percent AS frag
FROM sys.dm_db_index_physical_stats (DB_ID(), NULL, NULL , NULL, ''LIMITED'')
WHERE avg_fragmentation_in_percent > @fragrate AND index_id > 0

-- Declare the cursor for the list of partitions to be processed.
DECLARE partitions CURSOR FOR SELECT * FROM @indexinfo
OPEN partitions

-- Loop through the partitions.
FETCH NEXT FROM partitions
	INTO @objectid, @indexid, @partitionnum, @frag

WHILE (@@fetch_status <> -1)
    BEGIN
        IF (@@fetch_status <> -2)
		BEGIN

			SELECT @objectname = QUOTENAME(o.name), @schemaname = QUOTENAME(s.name)
			FROM sys.objects AS o
			JOIN sys.schemas as s ON s.schema_id = o.schema_id
			WHERE o.object_id = @objectid

			SELECT @indexname = QUOTENAME(name)
			FROM sys.indexes
			WHERE  object_id = @objectid AND index_id =@indexid

			SELECT @partitioncount = count (*)
			FROM sys.partitions
			WHERE object_id = @objectid AND index_id =@indexid

			-- 30 is an arbitrary decision point at which to switch between reorganizing and rebuilding.
			IF @frag < @rbrirate
				SET @command = N''ALTER INDEX '' + @indexname + N'' ON '' + @schemaname + N''.'' + @objectname + N'' REORGANIZE''
			IF @frag >= @rbrirate
				SET @command = N''ALTER INDEX '' + @indexname + N'' ON '' + @schemaname + N''.'' + @objectname + N'' REBUILD''
			IF @partitioncount > 1
				SET @command = @command + N'' PARTITION='' + CAST(@partitionnum AS nvarchar(10))

			EXEC (@command)
			PRINT N''Executed (Defrag '' + CAST(@frag AS varchar(12)) + ''): '' + @command
		END
		-- Get the next record to process
        FETCH NEXT FROM partitions
			INTO @objectid, @indexid, @partitionnum, @frag
    END

-- Close and deallocate the cursor.
CLOSE partitions
DEALLOCATE partitions', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Index Defrag on DB2]    Script Date: 4/20/2022 3:14:36 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Index Defrag on DB2', 
		@step_id=2, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'-- define index defrag rate for this run
DECLARE @fragrate float
SET @fragrate = 30.0 -- set to do something if defrag rate HIGH

DECLARE @rbrirate float
SET @rbrirate = 30.0 -- set to rebuild if defrag rate EXTREME

-- define table holding index information
DECLARE @indexinfo TABLE(objectid int, indexid int, partitionnum bigint, frag float)

DECLARE @objectid int
DECLARE @indexid int
DECLARE @partitioncount bigint
DECLARE @schemaname nvarchar(130) 
DECLARE @objectname nvarchar(130) 
DECLARE @indexname nvarchar(130) 
DECLARE @partitionnum bigint
DECLARE @partitions bigint
DECLARE @frag float
DECLARE @command nvarchar(4000) 

-- Conditionally select tables and indexes from the sys.dm_db_index_physical_stats function 
INSERT INTO @indexinfo
SELECT
    object_id AS objectid,
    index_id AS indexid,
    partition_number AS partitionnum,
    avg_fragmentation_in_percent AS frag
FROM sys.dm_db_index_physical_stats (DB_ID(), NULL, NULL , NULL, ''LIMITED'')
WHERE avg_fragmentation_in_percent > @fragrate AND index_id > 0

-- Declare the cursor for the list of partitions to be processed.
DECLARE partitions CURSOR FOR SELECT * FROM @indexinfo
OPEN partitions

-- Loop through the partitions.
FETCH NEXT FROM partitions
	INTO @objectid, @indexid, @partitionnum, @frag

WHILE (@@fetch_status <> -1)
    BEGIN
        IF (@@fetch_status <> -2)
		BEGIN

			SELECT @objectname = QUOTENAME(o.name), @schemaname = QUOTENAME(s.name)
			FROM sys.objects AS o
			JOIN sys.schemas as s ON s.schema_id = o.schema_id
			WHERE o.object_id = @objectid

			SELECT @indexname = QUOTENAME(name)
			FROM sys.indexes
			WHERE  object_id = @objectid AND index_id =@indexid

			SELECT @partitioncount = count (*)
			FROM sys.partitions
			WHERE object_id = @objectid AND index_id =@indexid

			-- 30 is an arbitrary decision point at which to switch between reorganizing and rebuilding.
			IF @frag < @rbrirate
				SET @command = N''ALTER INDEX '' + @indexname + N'' ON '' + @schemaname + N''.'' + @objectname + N'' REORGANIZE''
			IF @frag >= @rbrirate
				SET @command = N''ALTER INDEX '' + @indexname + N'' ON '' + @schemaname + N''.'' + @objectname + N'' REBUILD''
			IF @partitioncount > 1
				SET @command = @command + N'' PARTITION='' + CAST(@partitionnum AS nvarchar(10))

			EXEC (@command)
			PRINT N''Executed (Defrag '' + CAST(@frag AS varchar(12)) + ''): '' + @command
		END
		-- Get the next record to process
        FETCH NEXT FROM partitions
			INTO @objectid, @indexid, @partitionnum, @frag
    END

-- Close and deallocate the cursor.
CLOSE partitions
DEALLOCATE partitions', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Index defrag on DB3]    Script Date: 4/20/2022 3:14:36 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Index defrag on DB3', 
		@step_id=3, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'-- define index defrag rate for this run
DECLARE @fragrate float
SET @fragrate = 30.0 -- set to do something if defrag rate HIGH

DECLARE @rbrirate float
SET @rbrirate = 30.0 -- set to rebuild if defrag rate EXTREME

-- define table holding index information
DECLARE @indexinfo TABLE(objectid int, indexid int, partitionnum bigint, frag float)

DECLARE @objectid int
DECLARE @indexid int
DECLARE @partitioncount bigint
DECLARE @schemaname nvarchar(130) 
DECLARE @objectname nvarchar(130) 
DECLARE @indexname nvarchar(130) 
DECLARE @partitionnum bigint
DECLARE @partitions bigint
DECLARE @frag float
DECLARE @command nvarchar(4000) 

-- Conditionally select tables and indexes from the sys.dm_db_index_physical_stats function 
INSERT INTO @indexinfo
SELECT
    object_id AS objectid,
    index_id AS indexid,
    partition_number AS partitionnum,
    avg_fragmentation_in_percent AS frag
FROM sys.dm_db_index_physical_stats (DB_ID(), NULL, NULL , NULL, ''LIMITED'')
WHERE avg_fragmentation_in_percent > @fragrate AND index_id > 0

-- Declare the cursor for the list of partitions to be processed.
DECLARE partitions CURSOR FOR SELECT * FROM @indexinfo
OPEN partitions

-- Loop through the partitions.
FETCH NEXT FROM partitions
	INTO @objectid, @indexid, @partitionnum, @frag

WHILE (@@fetch_status <> -1)
    BEGIN
        IF (@@fetch_status <> -2)
		BEGIN

			SELECT @objectname = QUOTENAME(o.name), @schemaname = QUOTENAME(s.name)
			FROM sys.objects AS o
			JOIN sys.schemas as s ON s.schema_id = o.schema_id
			WHERE o.object_id = @objectid

			SELECT @indexname = QUOTENAME(name)
			FROM sys.indexes
			WHERE  object_id = @objectid AND index_id =@indexid

			SELECT @partitioncount = count (*)
			FROM sys.partitions
			WHERE object_id = @objectid AND index_id =@indexid

			-- 30 is an arbitrary decision point at which to switch between reorganizing and rebuilding.
			IF @frag < @rbrirate
				SET @command = N''ALTER INDEX '' + @indexname + N'' ON '' + @schemaname + N''.'' + @objectname + N'' REORGANIZE''
			IF @frag >= @rbrirate
				SET @command = N''ALTER INDEX '' + @indexname + N'' ON '' + @schemaname + N''.'' + @objectname + N'' REBUILD''
			IF @partitioncount > 1
				SET @command = @command + N'' PARTITION='' + CAST(@partitionnum AS nvarchar(10))

			EXEC (@command)
			PRINT N''Executed (Defrag '' + CAST(@frag AS varchar(12)) + ''): '' + @command
		END
		-- Get the next record to process
        FETCH NEXT FROM partitions
			INTO @objectid, @indexid, @partitionnum, @frag
    END

-- Close and deallocate the cursor.
CLOSE partitions
DEALLOCATE partitions', 
		@database_name=N'master', 
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


