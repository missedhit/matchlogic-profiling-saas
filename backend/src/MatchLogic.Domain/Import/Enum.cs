using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Domain.Import;
public enum DataSourceType : byte
{
    LiteDB,
    CSV = 1,
    Excel = 2,
    SQLServer = 3,
    MySQL = 4,
    PostgreSQL = 5,
    Snowflake = 6,
    Neo4j = 7,
    FTP = 8,
    SFTP = 9,
    S3 = 10,
    AzureBlob = 11,
    GoogleDrive = 12,
    Dropbox = 13,
    OneDrive = 14
}

public enum RunStatus : byte
{
    NotStarted = 1,
    InProgress = 2,
    Completed = 3,
    Failed = 4,
    Cancelled = 5
}

public enum StepType : byte
{
    Import = 10,
    Profile = 15,
    AdvanceProfile = 20,
    Cleanse = 25,
    Match = 30,
    Merge = 35,
    Overwrite = 37,
    Export = 40,
}

public enum JobStatus : byte
{
    Pending = 1,
    Running = 2,
    Completed = 3,
    Failed = 4,
    Skipped = 5
}

