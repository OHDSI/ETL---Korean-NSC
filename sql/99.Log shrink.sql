USE [NHIS_NSC];
GO
-- Truncate the log by changing the database recovery model to SIMPLE.
ALTER DATABASE [NHIS_NSC]
SET RECOVERY SIMPLE;
GO

-- Shrink the truncated log file to 1 MB.
DBCC SHRINKFILE (NHIS_NSC_log, 1);
GO

-- Reset the database recovery model.
ALTER DATABASE [NHIS_NSC]
SET RECOVERY FULL;
GO