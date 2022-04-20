-- define index defrag rate for this run
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
FROM sys.dm_db_index_physical_stats (DB_ID(), NULL, NULL , NULL, 'LIMITED')
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
				SET @command = N'ALTER INDEX ' + @indexname + N' ON ' + @schemaname + N'.' + @objectname + N' REORGANIZE'
			IF @frag >= @rbrirate
				SET @command = N'ALTER INDEX ' + @indexname + N' ON ' + @schemaname + N'.' + @objectname + N' REBUILD'
			IF @partitioncount > 1
				SET @command = @command + N' PARTITION=' + CAST(@partitionnum AS nvarchar(10))

			EXEC (@command)
			PRINT N'Executed (Defrag ' + CAST(@frag AS varchar(12)) + '): ' + @command
		END
		-- Get the next record to process
        FETCH NEXT FROM partitions
			INTO @objectid, @indexid, @partitionnum, @frag
    END

-- Close and deallocate the cursor.
CLOSE partitions
DEALLOCATE partitions