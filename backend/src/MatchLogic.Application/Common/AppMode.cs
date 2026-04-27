using System;

namespace MatchLogic.Application.Common;

public enum AppMode { Desktop, Server }

public class AppModeSettings
{
    public const string SectionName = "AppMode";
    public string Mode { get; set; } = nameof(AppMode.Server);

    public AppMode Resolved => Enum.TryParse<AppMode>(Mode, true, out var m) ? m : AppMode.Server;
    public bool IsDesktop => Resolved == AppMode.Desktop;
    public bool IsServer  => Resolved == AppMode.Server;

    /// <summary>Kestrel listen port for deployment mode.</summary>
    public int Port { get; set; } = 5000;

    /// <summary>Single-instance mutex name (must differ between Desktop and Server).</summary>
    public string MutexName { get; set; } = "MatchLogicApiSingleInstance";

    /// <summary>Named-pipe name for second-instance signaling.</summary>
    public string PipeName { get; set; } = "MatchLogicApiPipe";
}
