using System.Collections.Generic;

namespace MatchLogic.Application.Identity;

public static class AppPermissions
{
    public static class Projects
    {
        public const string Create = "projects.create";
        public const string Read   = "projects.read";
        public const string Update = "projects.update";
        public const string Delete = "projects.delete";
        public const string Run    = "projects.run";
    }

    public static class DataImport
    {
        public const string View    = "dataimport.view";
        public const string Execute = "dataimport.execute";
    }

    public static class Profiling
    {
        public const string View    = "profiling.view";
        public const string Execute = "profiling.execute";
    }

    public static class Cleansing
    {
        public const string View      = "cleansing.view";
        public const string Execute   = "cleansing.execute";
        public const string Configure = "cleansing.configure";
    }

    public static class Matching
    {
        public const string View      = "matching.view";
        public const string Execute   = "matching.execute";
        public const string Configure = "matching.configure";
    }

    public static class Survivorship
    {
        public const string View      = "survivorship.view";
        public const string Execute   = "survivorship.execute";
        public const string Configure = "survivorship.configure";
    }

    public static class Export
    {
        public const string View    = "export.view";
        public const string Execute = "export.execute";
    }

    public static class Scheduler
    {
        public const string Manage = "scheduler.manage";
        public const string View   = "scheduler.view";
    }

    public static class Dictionary
    {
        public const string View   = "dictionary.view";
        public const string Manage = "dictionary.manage";
    }

    public static class Admin
    {
        public const string HangfireDashboard = "admin.hangfire";
        public const string LicenseView       = "admin.license.view";
    }

    /// <summary>
    /// Default role → permission mapping.
    /// Used by PermissionAuthorizationHandler today.
    /// Will seed the DB permission table when the DB layer is added.
    /// Keys are Keycloak realm role names (lowercase, hyphenated).
    /// </summary>
    public static readonly Dictionary<string, string[]> DefaultRolePermissions = new()
    {
        ["admin"] = new[]
        {
            Projects.Create, Projects.Read, Projects.Update, Projects.Delete, Projects.Run,
            DataImport.View, DataImport.Execute,
            Profiling.View, Profiling.Execute,
            Cleansing.View, Cleansing.Execute, Cleansing.Configure,
            Matching.View, Matching.Execute, Matching.Configure,
            Survivorship.View, Survivorship.Execute, Survivorship.Configure,
            Export.View, Export.Execute,
            Scheduler.Manage, Scheduler.View,
            Dictionary.View, Dictionary.Manage,
            Admin.HangfireDashboard, Admin.LicenseView
        },
        ["manager"] = new[]
        {
            Projects.Create, Projects.Read, Projects.Update, Projects.Run,
            DataImport.View, DataImport.Execute,
            Profiling.View, Profiling.Execute,
            Cleansing.View, Cleansing.Execute, Cleansing.Configure,
            Matching.View, Matching.Execute, Matching.Configure,
            Survivorship.View, Survivorship.Execute, Survivorship.Configure,
            Export.View, Export.Execute,
            Scheduler.Manage, Scheduler.View,
            Dictionary.View, Dictionary.Manage
        },
        ["operator"] = new[]
        {
            Projects.Read,
            DataImport.View,
            Profiling.View, Profiling.Execute,
            Cleansing.View, Cleansing.Execute, Cleansing.Configure,
            Matching.View, Matching.Execute,
            Survivorship.View, Survivorship.Execute, Survivorship.Configure,
            Export.View, Export.Execute,
            Scheduler.View,
            Dictionary.View
        },
        ["reviewer"] = new[]
        {
            Projects.Read,
            DataImport.View,
            Profiling.View,
            Cleansing.View,
            Matching.View,
            Survivorship.View,
            Export.View,
            Scheduler.View,
            Dictionary.View
        },
        ["viewer"] = new[]
        {
            Projects.Read,
            DataImport.View,
            Profiling.View,
            Cleansing.View,
            Matching.View,
            Survivorship.View,
            Export.View,
            Scheduler.View,
            Dictionary.View
        }
    };

    /// <summary>
    /// All permission constants — used by RbacSetup to register one ASP.NET Core policy each.
    /// </summary>
    public static IEnumerable<string> All()
    {
        yield return Projects.Create;
        yield return Projects.Read;
        yield return Projects.Update;
        yield return Projects.Delete;
        yield return Projects.Run;
        yield return DataImport.View;
        yield return DataImport.Execute;
        yield return Profiling.View;
        yield return Profiling.Execute;
        yield return Cleansing.View;
        yield return Cleansing.Execute;
        yield return Cleansing.Configure;
        yield return Matching.View;
        yield return Matching.Execute;
        yield return Matching.Configure;
        yield return Survivorship.View;
        yield return Survivorship.Execute;
        yield return Survivorship.Configure;
        yield return Export.View;
        yield return Export.Execute;
        yield return Scheduler.Manage;
        yield return Scheduler.View;
        yield return Dictionary.View;
        yield return Dictionary.Manage;
        yield return Admin.HangfireDashboard;
        yield return Admin.LicenseView;
    }
}
