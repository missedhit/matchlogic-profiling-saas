using MatchLogic.Domain.Project;
using System;
using System.Collections.Generic;
using System.Text;

namespace MatchLogic.Domain.Import;

public class PostgresConnectionConfig : ConnectionConfig, IDBConnectionInfo
{
    #region Connection Constants
    // Basic Settings
    private const string ServerKey = "Server";
    private const string DatabaseKey = "Database";
    private const string UsernameKey = "Username";
    private const string PasswordKey = "Password";
    private const string PortKey = "Port";
    private const string ConnectionTimeoutKey = "ConnectionTimeout";
    
    // Entity Framework Settings
    private const string EfAdminDatabaseKey = "EfAdminDatabase";
    private const string EfTemplateDatabaseKey = "EfTemplateDatabase";
    
    // Pooling Settings
    private const string PoolingKey = "Pooling";
    private const string ConnectionIdleLifetimeKey = "ConnectionIdleLifetime";
    private const string ConnectionPruningIntervalKey = "ConnectionPruningInterval";
    private const string MaxPoolSizeKey = "MaxPoolSize";
    private const string MinPoolSizeKey = "MinPoolSize";
    
    // Security Settings
    private const string CheckCertificateRevocationKey = "CheckCertificateRevocation";
    private const string IncludeRealmKey = "IncludeRealm";
    private const string IntegratedSecurityKey = "IntegratedSecurity";
    private const string KerberosServiceNameKey = "KerberosServiceName";
    private const string PersistSecurityInfoKey = "PersistSecurityInfo";
    private const string SslModeKey = "SslMode";
    private const string TrustServerCertificateKey = "TrustServerCertificate";
    private const string UseSslStreamKey = "UseSslStream";
    
    // Timeout Settings
    private const string CommandTimeoutKey = "CommandTimeout";
    private const string InternalCommandTimeoutKey = "InternalCommandTimeout";
    private const string TimeoutKey = "Timeout";
    
    // Compatibility Settings
    private const string ConvertInfinityDateTimeKey = "ConvertInfinityDateTime";
    private const string ServerCompatibilityModeKey = "ServerCompatibilityMode";
    
    // Connection Settings
    private const string ApplicationNameKey = "ApplicationName";
    private const string ClientEncodingKey = "ClientEncoding";
    private const string EncodingKey = "Encoding";
    private const string EnlistKey = "Enlist";
    private const string PassfileKey = "Passfile";
    private const string SearchPathKey = "SearchPath";
    private const string TimezoneKey = "Timezone";
    
    // Advanced Settings
    private const string AutoPrepareMinUsagesKey = "AutoPrepareMinUsages";
    private const string KeepaliveKey = "Keepalive";
    private const string LoadTableCompositesKey = "LoadTableComposites";
    private const string MaxAutoPrepareKey = "MaxAutoPrepare";
    private const string NoResetOnCloseKey = "NoResetOnClose";
    private const string ReadBufferSizeKey = "ReadBufferSize";
    private const string SocketReceiveBufferSizeKey = "SocketReceiveBufferSize";
    private const string SocketSendBufferSizeKey = "SocketSendBufferSize";
    private const string TcpKeepaliveKey = "TcpKeepalive";
    private const string TcpKeepaliveIntervalKey = "TcpKeepaliveInterval";
    private const string TcpKeepaliveTimeKey = "TcpKeepaliveTime";
    private const string UsePerfCountersKey = "UsePerfCounters";
    private const string WriteBufferSizeKey = "WriteBufferSize";
    #endregion

    public override bool CanCreateFromArgs(DataSourceType Type)
        => Type == DataSourceType.PostgreSQL;

    #region Properties 
    public string ConnectionString
    {
        get
        {   
            // Build Connection String
            var builder = new StringBuilder();
            
            // Basic Connection Parameters
            builder.Append($"Host={Parameters[ServerKey]};");
            
            if (Parameters.ContainsKey(PortKey) && int.TryParse(Parameters[PortKey], out var port))
                builder.Append($"Port={port};");
            else
                builder.Append("Port=5432;"); // Default PostgreSQL port
                
            if (Parameters.ContainsKey(DatabaseKey))
                builder.Append($"Database={Parameters[DatabaseKey]};");


            builder.Append($"Username={Parameters[UsernameKey]};");
            builder.Append($"Password={Parameters[PasswordKey]};");


            // Add Pooling Settings
            AppendBoolParameter(builder, PoolingKey);
            AppendIntParameter(builder, ConnectionIdleLifetimeKey);
            AppendIntParameter(builder, ConnectionPruningIntervalKey);
            AppendIntParameter(builder, MaxPoolSizeKey);
            AppendIntParameter(builder, MinPoolSizeKey);
            
            // Add Security Settings
            AppendBoolParameter(builder, CheckCertificateRevocationKey);
            AppendBoolParameter(builder, IncludeRealmKey);
            AppendBoolParameter(builder, PersistSecurityInfoKey);
            AppendStringParameter(builder, KerberosServiceNameKey);
            AppendStringParameter(builder, SslModeKey);
            AppendBoolParameter(builder, TrustServerCertificateKey);
            AppendBoolParameter(builder, UseSslStreamKey);
            
            // Add Timeout Settings
            AppendIntParameter(builder, CommandTimeoutKey);
            AppendIntParameter(builder, InternalCommandTimeoutKey);
            AppendIntParameter(builder, TimeoutKey);
            
            // Add Compatibility Settings
            AppendBoolParameter(builder, ConvertInfinityDateTimeKey);
            AppendStringParameter(builder, ServerCompatibilityModeKey);

            // Add Entity Framework Settings
            AppendStringParameter(builder, EfAdminDatabaseKey);
            AppendStringParameter(builder, EfTemplateDatabaseKey);

            // Add Connection Settings
            AppendStringParameter(builder, ApplicationNameKey);
            AppendStringParameter(builder, ClientEncodingKey);
            AppendStringParameter(builder, EncodingKey);
            AppendBoolParameter(builder, EnlistKey);
            AppendStringParameter(builder, PassfileKey);
            AppendStringParameter(builder, SearchPathKey);
            AppendStringParameter(builder, TimezoneKey);
            
            // Add Advanced Settings
            AppendIntParameter(builder, AutoPrepareMinUsagesKey);
            AppendIntParameter(builder, KeepaliveKey);
            AppendBoolParameter(builder, LoadTableCompositesKey);
            AppendIntParameter(builder, MaxAutoPrepareKey);
            AppendBoolParameter(builder, NoResetOnCloseKey);
            AppendIntParameter(builder, ReadBufferSizeKey);
            AppendIntParameter(builder, SocketReceiveBufferSizeKey);
            AppendIntParameter(builder, SocketSendBufferSizeKey);
            AppendBoolParameter(builder, TcpKeepaliveKey);
            AppendIntParameter(builder, TcpKeepaliveIntervalKey);
            AppendIntParameter(builder, TcpKeepaliveTimeKey);
            AppendBoolParameter(builder, UsePerfCountersKey);
            AppendIntParameter(builder, WriteBufferSizeKey);
            
            // Add Connection Timeout
            if (Parameters.ContainsKey(ConnectionTimeoutKey) && int.TryParse(Parameters[ConnectionTimeoutKey], out int timeout))
                builder.Append($"Timeout={timeout};");
                
            return builder.ToString();
        }
    }

    private void AppendStringParameter(StringBuilder builder, string key)
    {
        if (Parameters.ContainsKey(key) && !string.IsNullOrEmpty(Parameters[key]))
            builder.Append($"{key}={Parameters[key]};");
    }

    private void AppendBoolParameter(StringBuilder builder, string key)
    {
        if (Parameters.ContainsKey(key) && bool.TryParse(Parameters[key], out bool value))
            builder.Append($"{key}={value.ToString().ToLower()};");
    }

    private void AppendIntParameter(StringBuilder builder, string key)
    {
        if (Parameters.ContainsKey(key) && int.TryParse(Parameters[key], out int value))
            builder.Append($"{key}={value};");
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
    
    // If tableName contains schema (e.g. "public.MyTable"), split it
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
    
    // If tableName contains schema (e.g. "public.MyTable"), split it
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
                return "public"; // Default schema in PostgreSQL
            }
        }
    }
    
    public TimeSpan ConnectionTimeout
    {
        get
        {
            // First check the specific timeout key
            if (Parameters.ContainsKey(TimeoutKey) && int.TryParse(Parameters[TimeoutKey], out var timeout))
                return TimeSpan.FromSeconds(timeout);
                
            // For backward compatibility
            if (Parameters.ContainsKey(ConnectionTimeoutKey) && int.TryParse(Parameters[ConnectionTimeoutKey], out var legacyTimeout))
                return TimeSpan.FromSeconds(legacyTimeout);
                
            return TimeSpan.FromMinutes(30); // Default timeout
        }
    }
    #endregion
    
    public override ConnectionConfig CreateFromArgs(DataSourceType Type, Dictionary<string, string> args, DataSourceConfiguration? sourceConfiguration = null)
    {
        if (Type != DataSourceType.PostgreSQL)
        {
            throw new ArgumentException("Invalid data source type for PostgresConnectionConfig");
        }
        
        Parameters = args;
        SourceConfig = sourceConfiguration;
        
        // Required parameters validation
        if (!Parameters.ContainsKey(ServerKey) || string.IsNullOrEmpty(Parameters[ServerKey]))
        {
            throw new ArgumentException("Server is required for PostgreSQL connection");
        }        

        if (!Parameters.ContainsKey(UsernameKey) || string.IsNullOrEmpty(Parameters[UsernameKey]))
        {
            throw new ArgumentException("Username is required for SQL Authentication");
        }
        if (!Parameters.ContainsKey(PasswordKey) || string.IsNullOrEmpty(Parameters[PasswordKey]))
        {
            throw new ArgumentException("Password is required for SQL Authentication");
        }

        return this;
    }
}
