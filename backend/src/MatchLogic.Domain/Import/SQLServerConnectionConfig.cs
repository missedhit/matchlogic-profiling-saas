using MatchLogic.Domain.Project;
using System;
using System.Collections.Generic;
using System.Text;

namespace MatchLogic.Domain.Import;

public class SQLServerConnectionConfig : ConnectionConfig, IDBConnectionInfo
{
    #region Properties 
    public string ConnectionString
    {
        get
        {   // Build Connection String
            var builder = new StringBuilder();
            builder.Append($"Data Source={Parameters[ServerKey]}");

            if (Parameters.ContainsKey(PortKey) && !string.IsNullOrEmpty(Parameters[PortKey]))
            {
                builder.Append($",{Parameters[PortKey]}");
            }

            builder.Append(";");

            if (Parameters.ContainsKey(DatabaseKey))
                builder.Append($"Initial Catalog={Parameters[DatabaseKey]};");

            if (Parameters[AuthTypeKey] == "SQL")
            {
                builder.Append($"User ID={Parameters[UsernameKey]};");
                builder.Append($"Password={Parameters[PasswordKey]};");
            }
            else
            {
                builder.Append("Integrated Security=True;");
            }

            if (Parameters.ContainsKey(TrustServerCertificateKey))
                builder.Append($"TrustServerCertificate={Parameters[TrustServerCertificateKey]};");

            return builder.ToString();
        }
    }

    public string Query
    {
        get {
            if (SourceConfig != null && SourceConfig.Query != null)
                return SourceConfig.Query;
            return string.Empty;

        }
    }
    // If tableName contains schema (e.g. "dbo.MyTable"), split it
    public string TableName
    {
        get
        {
            if (SourceConfig == null)
                return string.Empty;
            if (SourceConfig.TableOrSheet == null)
                return string.Empty;

            if (SourceConfig.TableOrSheet.Contains('.'))
            {
                return SourceConfig.TableOrSheet.Split('.')[1];
            }
            else
            {
                return SourceConfig.TableOrSheet;
            }
        }
    }
    // If tableName contains schema (e.g. "dbo.MyTable"), split it
    public string SchemaName
    {
        get
        {
            if (SourceConfig != null && SourceConfig.TableOrSheet != null && SourceConfig.TableOrSheet.Contains('.'))
            {
                return SourceConfig.TableOrSheet.Split('.')[0];
            }
            else
            {
                return string.Empty;
            }
        }
    }

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


    private const string ServerKey = "Server";
    private const string DatabaseKey = "Database";
    private const string UsernameKey = "Username";
    private const string PasswordKey = "Password";
    private const string AuthTypeKey = "AuthType";
    private const string PortKey = "Port";
    private const string TrustServerCertificateKey = "TrustServerCertificate";
    private const string ConnectionTimeoutKey = "ConnectionTimeout";

    public override bool CanCreateFromArgs(DataSourceType Type)
        => Type == DataSourceType.SQLServer;
    public override ConnectionConfig CreateFromArgs(DataSourceType Type, Dictionary<string, string> args, DataSourceConfiguration? sourceConfiguration = null)
    {
        if (Type != DataSourceType.SQLServer)
        {
            throw new ArgumentException("Invalid data source type for SQLServerConnectionConfig");
        }
        Parameters = args;
        SourceConfig = sourceConfiguration;
        #region
        if (!Parameters.ContainsKey(ServerKey) || string.IsNullOrEmpty(Parameters[ServerKey]))
        {
            throw new ArgumentException("Server is required for SQL Server connection");
        }
        if (!Parameters.ContainsKey(AuthTypeKey) || string.IsNullOrEmpty(Parameters[AuthTypeKey]))
        {
            throw new ArgumentException("AuthType is required for SQL Server connection");
        }
        if (Parameters[AuthTypeKey] == "SQL")
        {
            if (!Parameters.ContainsKey(UsernameKey) || string.IsNullOrEmpty(Parameters[UsernameKey]))
            {
                throw new ArgumentException("Username is required for SQL Authentication");
            }
            if (!Parameters.ContainsKey(PasswordKey) || string.IsNullOrEmpty(Parameters[PasswordKey]))
            {
                throw new ArgumentException("Password is required for SQL Authentication");
            }
        }
        #endregion


        return this;
    }

    public override bool ValidateConnection()
    {
        if (!base.ValidateConnection())
            return false;

        var parameters = Parameters;

        // Fast fail for required keys
        if (!parameters.TryGetValue(ServerKey, out var server) || string.IsNullOrEmpty(server))
            return false;
        //if (!parameters.TryGetValue(DatabaseKey, out var database) || string.IsNullOrEmpty(database))
        //    return false;
        if (!parameters.TryGetValue(AuthTypeKey, out var authType) || string.IsNullOrEmpty(authType))
            return false;

        if (authType == "SQL")
        {
            if (!parameters.TryGetValue(UsernameKey, out var username) || string.IsNullOrEmpty(username))
                return false;
            if (!parameters.TryGetValue(PasswordKey, out var password) || string.IsNullOrEmpty(password))
                return false;
        }

        if (SourceConfig!=null  && !string.IsNullOrEmpty(SourceConfig.Query))
            return this.IsValidSelectQuery();


        return true;
    }
}
