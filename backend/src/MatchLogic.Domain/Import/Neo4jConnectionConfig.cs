using MatchLogic.Domain.Project;
using System;
using System.Collections.Generic;

namespace MatchLogic.Domain.Import;

public class Neo4jConnectionConfig : ConnectionConfig
{
    #region Connection Constants
    private const string ServerKey = "Server";
    private const string PortKey = "Port";
    private const string ProtocolKey = "Protocol";
    private const string UsernameKey = "Username";
    private const string PasswordKey = "Password";
    private const string DatabaseKey = "Database";
    private const string EncryptionKey = "Encryption";
    private const string TrustCertificateKey = "TrustCertificate";
    private const string ConnectionTimeoutKey = "ConnectionTimeout";
    #endregion

    #region Export Setting Constants
    public const string ExportModeKey = "Neo4j.ExportMode";
    public const string NodeLabelKey = "Neo4j.NodeLabel";
    public const string RelationshipTypeKey = "Neo4j.RelationshipType";
    public const string BatchSizeKey = "Neo4j.BatchSize";
    public const string CreateIndexesKey = "Neo4j.CreateIndexes";
    public const string ClearExistingKey = "Neo4j.ClearExisting";
    #endregion

    #region Default Values
    public const int DefaultPort = 7687;
    public const string DefaultProtocol = "bolt://";
    public const string DefaultUsername = "neo4j";
    public const string DefaultDatabase = "neo4j";
    public const string DefaultExportMode = "graph";
    public const string DefaultNodeLabel = "Record";
    public const string DefaultRelationshipType = "MATCHED_WITH";
    public const int DefaultBatchSize = 1000;
    public const bool DefaultCreateIndexes = true;
    public const bool DefaultClearExisting = false;
    #endregion

    public override bool CanCreateFromArgs(DataSourceType Type)
        => Type == DataSourceType.Neo4j;

    #region Properties
    public string Server => Parameters.ContainsKey(ServerKey) ? Parameters[ServerKey] : string.Empty;

    public int Port
    {
        get
        {
            if (Parameters.ContainsKey(PortKey) && int.TryParse(Parameters[PortKey], out var port))
                return port;
            return DefaultPort;
        }
    }

    public string Protocol
    {
        get
        {
            if (Parameters.ContainsKey(ProtocolKey) && !string.IsNullOrEmpty(Parameters[ProtocolKey]))
                return Parameters[ProtocolKey];
            return DefaultProtocol;
        }
    }

    public string Username
    {
        get
        {
            if (Parameters.ContainsKey(UsernameKey) && !string.IsNullOrEmpty(Parameters[UsernameKey]))
                return Parameters[UsernameKey];
            return DefaultUsername;
        }
    }

    public string Password => Parameters.ContainsKey(PasswordKey) ? Parameters[PasswordKey] : string.Empty;

    public string Database
    {
        get
        {
            if (Parameters.ContainsKey(DatabaseKey) && !string.IsNullOrEmpty(Parameters[DatabaseKey]))
                return Parameters[DatabaseKey];
            return DefaultDatabase;
        }
    }

    public bool Encryption
    {
        get
        {
            // Auto-enable if protocol uses TLS (bolt+s:// or neo4j+s://)
            if (Protocol.Contains("+s://"))
                return true;
            if (Parameters.ContainsKey(EncryptionKey) && bool.TryParse(Parameters[EncryptionKey], out var value))
                return value;
            return false;
        }
    }

    public bool TrustCertificate
    {
        get
        {
            if (Parameters.ContainsKey(TrustCertificateKey) && bool.TryParse(Parameters[TrustCertificateKey], out var value))
                return value;
            return false;
        }
    }

    public TimeSpan ConnectionTimeout
    {
        get
        {
            if (Parameters.ContainsKey(ConnectionTimeoutKey) && int.TryParse(Parameters[ConnectionTimeoutKey], out var timeout))
                return TimeSpan.FromSeconds(timeout);
            return TimeSpan.FromSeconds(30);
        }
    }

    /// <summary>
    /// Computed URI for Neo4j driver: {Protocol}{Server}:{Port}
    /// </summary>
    public string Uri => $"{Protocol}{Server}:{Port}";

    // Export settings
    public string ExportMode
    {
        get
        {
            if (Parameters.ContainsKey(ExportModeKey) && !string.IsNullOrEmpty(Parameters[ExportModeKey]))
                return Parameters[ExportModeKey];
            return DefaultExportMode;
        }
    }

    public string NodeLabel
    {
        get
        {
            if (Parameters.ContainsKey(NodeLabelKey) && !string.IsNullOrEmpty(Parameters[NodeLabelKey]))
                return Parameters[NodeLabelKey];
            return DefaultNodeLabel;
        }
    }

    public string RelationshipType
    {
        get
        {
            if (Parameters.ContainsKey(RelationshipTypeKey) && !string.IsNullOrEmpty(Parameters[RelationshipTypeKey]))
                return Parameters[RelationshipTypeKey];
            return DefaultRelationshipType;
        }
    }

    public int BatchSize
    {
        get
        {
            if (Parameters.ContainsKey(BatchSizeKey) && int.TryParse(Parameters[BatchSizeKey], out var value))
                return value;
            return DefaultBatchSize;
        }
    }

    public bool CreateIndexes
    {
        get
        {
            if (Parameters.ContainsKey(CreateIndexesKey) && bool.TryParse(Parameters[CreateIndexesKey], out var value))
                return value;
            return DefaultCreateIndexes;
        }
    }

    public bool ClearExisting
    {
        get
        {
            if (Parameters.ContainsKey(ClearExistingKey) && bool.TryParse(Parameters[ClearExistingKey], out var value))
                return value;
            return DefaultClearExisting;
        }
    }
    #endregion

    public override ConnectionConfig CreateFromArgs(DataSourceType Type, Dictionary<string, string> args, DataSourceConfiguration? sourceConfiguration = null)
    {
        if (Type != DataSourceType.Neo4j)
        {
            throw new ArgumentException("Invalid data source type for Neo4jConnectionConfig");
        }

        Parameters = args;
        SourceConfig = sourceConfiguration;

        // Required parameters validation
        if (!Parameters.ContainsKey(ServerKey) || string.IsNullOrEmpty(Parameters[ServerKey]))
        {
            throw new ArgumentException("Server is required for Neo4j connection");
        }

        if (!Parameters.ContainsKey(PasswordKey) || string.IsNullOrEmpty(Parameters[PasswordKey]))
        {
            throw new ArgumentException("Password is required for Neo4j connection");
        }

        return this;
    }
}
