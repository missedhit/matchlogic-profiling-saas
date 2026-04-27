namespace MatchLogic.Api.Common;

public static class ValidationMessages
{
    // Parameterized methods for common patterns
    public static string NotExists(string entity) => $"{entity} does not exist.";    
    public static string Required(string field) => $"{field} is required.";
    public static string AlreadyExists(string entity) => $"{entity} with this name already exists.";
    public static string MaxLength(string field, int length) => $"{field} must not exceed {length} characters.";
    public static string MustBeUnique(string field) => $"{field} must contain unique values.";
    public static string CannotBeNull(string field) => $"{field} cannot be null.";
    public static string CannotBeEmpty(string field) => $"{field} cannot be empty.";
    public static string CannotContainEmptyOrWhitespace(string field) => $"{field} cannot contain empty or whitespace values.";
    public static string Invalid(string field) => $"Invalid {field}.";
    public static string NotUploaded(string field) => $"No {field} was uploaded.";
    public static string IsEmpty(string field) => $"{field} is empty";
    public static string NotAllowed(string field) => $"{field} is not allowed.";
    public static string NotAllowedWithList(string field, string allowed) => $"{field} is not allowed. Allowed values are: {allowed}";
    public static string ModificationNotAllowed(string entity) => $"Modification of items in a {entity} is not allowed.";
    public static string NotFoundFor(string field) => $"No data found for {field}";
    public static string MustBeUniqueInList(string field) => $"{field} should be unique in List.";
    public static string InvalidForSpecified(string entity, string specified) => $"One or more {entity} do not exist or are invalid for the specified {specified}.";

}