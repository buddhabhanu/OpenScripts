
Set quoted_identifier off
use master
go


DECLARE @fillfactor varchar(5)
DECLARE @tablename varchar(300)
DECLARE @tablename_header varchar(300)
DECLARE @dataname varchar(300)
DECLARE @dataname_header varchar(300)
DECLARE datanames_cursor CURSOR FOR SELECT name FROM sys.databases
        WHERE  user_access_desc='MULTI_USER' and database_id>2  -- name not in ('master', 'pubs', 'tempdb', 'model', 'northwind')
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
EXEC ("USE " + @dataname + " DECLARE tnames_cursor CURSOR FOR SELECT name from sysobjects where type = 'U' order by name ")
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
		  
--EXEC ("USE " + @dataname + " DBCC DBREINDEX (" + @tablename + "," + "''" + "," + @fillfactor + ")")
EXEC ("USE " + @dataname + " UPDATE STATISTICS [" + @tablename + "] WITH SAMPLE 20 PERCENT")
FETCH NEXT FROM tnames_cursor INTO @tablename
        END
DEALLOCATE tnames_cursor

      FETCH NEXT FROM datanames_cursor INTO @dataname
      END



DEALLOCATE datanames_cursor
PRINT ""
PRINT " "
PRINT "Update Stats Completed on All Read write User Databases"