using MatchLogic.Domain.Project;
using System;
using System.Collections.Generic;
using System.Text;

namespace MatchLogic.Domain.Import;

public class MySQLConnectionConfig : ConnectionConfig, IDBConnectionInfo
{
    private const string ServerKey = "Server";
    private const string DatabaseKey = "Database";
    private const string UsernameKey = "Username";
    private const string PasswordKey = "Password";
    private const string PortKey = "Port";
    private const string TrustServerCertificateKey = "TrustServerCertificate";
    private const string ConnectionTimeoutKey = "ConnectionTimeout";
    public override bool CanCreateFromArgs(DataSourceType Type) => Type == DataSourceType.MySQL;
    //mysqlconnector
    #region Properties 
    public string ConnectionString
    {
        get
        {   // Build Connection String
            var builder = new StringBuilder();
            builder.Append($"Server={Parameters[ServerKey]}");
            if (Parameters.ContainsKey(PortKey) && !string.IsNullOrEmpty(Parameters[PortKey]))
            {
                builder.Append($";Port={Parameters[PortKey]}");
            }
            if (Parameters.ContainsKey(DatabaseKey))
                builder.Append($";Database={Parameters[DatabaseKey]}");


            builder.Append($";User ID={Parameters[UsernameKey]}");
            builder.Append($";Password={Parameters[PasswordKey]}");

            if (Parameters.ContainsKey(TrustServerCertificateKey))
                builder.Append($";SslMode={(Parameters[TrustServerCertificateKey].ToLower() == "true" ? "Required" : "None")}");
            return builder.ToString();
        }
    }
    public string Query
    {
        get
        {
            if (SourceConfig != null && SourceConfig.Query != null)
                return SourceConfig.Query;
            return string.Empty;
        }
    }
    public string TableName
    {
        get
        {
            if (SourceConfig == null)
                return string.Empty;
            if (SourceConfig.TableOrSheet == null)
                return string.Empty;
            return SourceConfig.TableOrSheet;
        }
    }
    public string SchemaName => string.Empty; // MySQL does not use schemas in the same way as SQL Server
    public TimeSpan ConnectionTimeout
    {
        get
        {
            if (Parameters.ContainsKey(ConnectionTimeoutKey) && int.TryParse(Parameters[ConnectionTimeoutKey], out var timeout))
                return TimeSpan.FromSeconds(timeout);
            return TimeSpan.FromMinutes(30); // Default timeout
        }
    }
    #endregion

    public override ConnectionConfig CreateFromArgs(DataSourceType Type, Dictionary<string, string> args, DataSourceConfiguration? sourceConfiguration = null)
    {
        if (Type != DataSourceType.MySQL)
        {
            throw new ArgumentException("Invalid data source type for MySQLConnectionConfig");
        }
        Parameters = args;
        SourceConfig = sourceConfiguration;
        #region
        if (!Parameters.ContainsKey(ServerKey) || string.IsNullOrEmpty(Parameters[ServerKey]))
        {
            throw new ArgumentException("Server is required for MySQL connection");
        }
        if (!Parameters.ContainsKey(UsernameKey) || string.IsNullOrEmpty(Parameters[UsernameKey]))
        {
            throw new ArgumentException("Username is required for SQL Authentication");
        }
        if (!Parameters.ContainsKey(PasswordKey) || string.IsNullOrEmpty(Parameters[PasswordKey]))
        {
            throw new ArgumentException("Password is required for SQL Authentication");
        }
        #endregion
        return this;
    }
}