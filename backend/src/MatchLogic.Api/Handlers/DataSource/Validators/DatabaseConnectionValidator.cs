using MatchLogic.Api.Common;
using MatchLogic.Domain.Project;
using FluentValidation;
using System.Collections.Generic;

namespace MatchLogic.Api.Handlers.DataSource.Validators;

public class DatabaseConnectionValidator : AbstractValidator<BaseConnectionInfo>
{
    private const string ServerKey = "Server";
    //private const string DatabaseKey = "Database";
    private const string UsernameKey = "Username";
    private const string PasswordKey = "Password";
    private const string AuthTypeKey = "AuthType";

    public DatabaseConnectionValidator()
    {
        RuleFor(x => x.Parameters)
            .Must(HasServer)
            .WithMessage(ValidationMessages.Required($"{ServerKey} parameter"));

        When(x => x.Type == Domain.Import.DataSourceType.SQLServer, () => // MS SQL required AuthType
        {
            RuleFor(x => x.Parameters)
                .Must(HasAuthType)
                .WithMessage(ValidationMessages.Required($"{AuthTypeKey} parameter"));

            RuleFor(x => x.Parameters)
               .Must(HasValidAuthentication)
               .WithMessage($"Either IntegratedSecurity=true or both {UsernameKey} and {PasswordKey} must be provided.");
        });

        When(x => x.Type != Domain.Import.DataSourceType.SQLServer, () => // Other Database Required UserName and Password Keys
        {
            RuleFor(x => x.Parameters)
                .Must(HasUserNameAndPassword)
                .WithMessage($"{UsernameKey} and {PasswordKey} must be provided.");
        });


        When(x => x.Parameters.ContainsKey("Port"), () =>
        {
            RuleFor(x => x.Parameters)
                .Must(parameters => parameters.TryGetValue("Port", out var portStr) && !string.IsNullOrWhiteSpace(portStr))
                .WithMessage(ValidationMessages.CannotContainEmptyOrWhitespace("Port parameter"));

            RuleFor(x => x.Parameters)
                .Must(parameters => parameters.TryGetValue("Port", out var portStr) && int.TryParse(portStr, out var port) && port > 0 && port <= 65535)
                .WithMessage(ValidationMessages.Invalid("Port parameter"));
        });

    }

    private static bool HasServer(Dictionary<string, string> parameters) =>
        parameters.TryGetValue(ServerKey, out var server) && !string.IsNullOrWhiteSpace(server);

    //private static bool HasDatabase(Dictionary<string, string> parameters) =>
    //    parameters.TryGetValue(DatabaseKey, out var db) && !string.IsNullOrWhiteSpace(db);

    private static bool HasAuthType(Dictionary<string, string> parameters) =>
        parameters.TryGetValue(AuthTypeKey, out var db) && !string.IsNullOrWhiteSpace(db);

    private static bool HasValidAuthentication(Dictionary<string, string> parameters)
    {
        if (parameters.TryGetValue(AuthTypeKey, out var auth) &&
            !string.IsNullOrEmpty(auth) && auth.ToLower() =="windows")
        {
            return true;
        }

        return HasUserNameAndPassword(parameters);
    }


    private static bool HasUserNameAndPassword(Dictionary<string, string> parameters)
    {       
        return parameters.TryGetValue(UsernameKey, out var user) && !string.IsNullOrWhiteSpace(user)
            && parameters.TryGetValue(PasswordKey, out var pwd) && !string.IsNullOrWhiteSpace(pwd);

    }



}
