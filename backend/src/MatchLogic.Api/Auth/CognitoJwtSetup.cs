using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.IdentityModel.Tokens;

namespace MatchLogic.Api.Auth;

public static class CognitoJwtSetup
{
    public static IServiceCollection AddCognitoJwtAuth(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        var options = configuration
            .GetSection(CognitoOptions.SectionName)
            .Get<CognitoOptions>() ?? new CognitoOptions();

        services.Configure<CognitoOptions>(configuration.GetSection(CognitoOptions.SectionName));

        var auth = services.AddAuthentication(JwtBearerDefaults.AuthenticationScheme);

        // If Cognito isn't configured (e.g. local dev before M1c provisioning),
        // still register the scheme so [Authorize] endpoints fail closed with 401
        // rather than throwing scheme-not-found. Tokens won't validate until config lands.
        if (!options.IsConfigured)
        {
            auth.AddJwtBearer(o =>
            {
                o.TokenValidationParameters = new TokenValidationParameters
                {
                    ValidateIssuer = false,
                    ValidateAudience = false,
                    ValidateLifetime = false,
                    ValidateIssuerSigningKey = false,
                    SignatureValidator = (token, _) => new Microsoft.IdentityModel.JsonWebTokens.JsonWebToken(token),
                };
            });
            return services;
        }

        auth.AddJwtBearer(o =>
        {
            o.Authority = options.Authority;
            o.RequireHttpsMetadata = true;
            o.IncludeErrorDetails = false;

            // Cognito access tokens have client_id (not aud); id tokens have aud=ClientId.
            // We validate against ClientId in OnTokenValidated to cover both cases.
            o.TokenValidationParameters = new TokenValidationParameters
            {
                ValidateIssuer = true,
                ValidIssuer = options.Authority,
                ValidateAudience = false,
                ValidateLifetime = true,
                ValidateIssuerSigningKey = true,
                ClockSkew = TimeSpan.FromMinutes(2),
            };

            o.Events = new JwtBearerEvents
            {
                OnTokenValidated = ctx =>
                {
                    var clientId = ctx.Principal?.FindFirst("client_id")?.Value
                                   ?? ctx.Principal?.FindFirst("aud")?.Value;

                    if (!string.Equals(clientId, options.ClientId, StringComparison.Ordinal))
                    {
                        ctx.Fail("Token client_id/aud does not match configured Cognito ClientId.");
                    }

                    return Task.CompletedTask;
                },
            };
        });

        return services;
    }
}
