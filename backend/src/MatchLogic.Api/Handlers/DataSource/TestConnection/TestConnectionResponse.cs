namespace MatchLogic.Api.Handlers.DataSource.TestConnection;

public record TestConnectionResponse(bool IsSuccessful, string? ErrorMessage = null);