using MatchLogic.Api.Handlers.MappedFieldRow.AutoMapping;
using MatchLogic.Application.Common;
using MatchLogic.Application.Features.CleansingAndStandardization.DTOs;
using MatchLogic.Application.Features.MatchDefinition.DTOs;
using MatchLogic.Application.Interfaces.Cleansing;
using MatchLogic.Application.Interfaces.MatchConfiguration;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.CleansingAndStandaradization;
using MatchLogic.Infrastructure.CleansingAndStandardization.CleansingRuleHandlers.WordSmith;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipelines;
using System.Linq;
using System.Reflection.Metadata;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.Cleansing.WordSmith.Upload;

public class UploadWordSmithDictionaryCommandHandler : IRequestHandler<UploadWordSmithDictionaryCommand, Result<WordSmithDictionaryResponse>>
{
    private readonly IWordSmithDictionaryService _service;
    private readonly IGenericRepository<WordSmithDictionary,Guid> _wordSmithDictionaryRepository;
    private readonly WordSmithDictionaryLoader _loader;
    private readonly ILogger<UploadWordSmithDictionaryCommandHandler> _logger;

    public UploadWordSmithDictionaryCommandHandler(
        IWordSmithDictionaryService service,
        WordSmithDictionaryLoader loader,
        IGenericRepository<WordSmithDictionary, Guid> wordSmithDictionaryRepository,
        ILogger<UploadWordSmithDictionaryCommandHandler> logger)
    {
        _service = service ?? throw new ArgumentNullException(nameof(service));
        _loader = loader ?? throw new ArgumentNullException(nameof(loader));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _wordSmithDictionaryRepository = wordSmithDictionaryRepository;
    }

    public async Task<Result<WordSmithDictionaryResponse>> Handle(
        UploadWordSmithDictionaryCommand request,
        CancellationToken cancellationToken)
    {
        try
        {
            // Validate input
            if (request.File == null || request.File.Length == 0)
            {
                return Result<WordSmithDictionaryResponse>.Error("File is required");
            }

            if (string.IsNullOrWhiteSpace(request.Name))
            {
                return Result<WordSmithDictionaryResponse>.Error("Dictionary name is required");
            }

            // Validate file extension
            var allowedExtensions = new[] { ".tsv", ".csv", ".txt" };
            var extension = Path.GetExtension(request.File.FileName).ToLower();
            if (!allowedExtensions.Contains(extension))
            {
                return Result<WordSmithDictionaryResponse>.Error(
                    $"Invalid file type. Allowed types: {string.Join(", ", allowedExtensions)}");
            }

            // Check file size (e.g., max 10MB)
            const int maxFileSize = 10 * 1024 * 1024; // 10MB
            if (request.File.Length > maxFileSize)
            {
                return Result<WordSmithDictionaryResponse>.Error(
                    $"File size exceeds maximum allowed size of {maxFileSize / (1024 * 1024)}MB");
            }

            // Resolve encoding up-front so invalid names surface as a validation error, not a 500.
            Encoding? explicitEncoding = null;
            if (!string.IsNullOrWhiteSpace(request.Encoding))
            {
                try
                {
                    explicitEncoding = Encoding.GetEncoding(request.Encoding);
                }
                catch (ArgumentException)
                {
                    return Result<WordSmithDictionaryResponse>.Error(
                        $"Invalid encoding '{request.Encoding}'. Provide a valid .NET encoding name (e.g. utf-8, utf-16, windows-1252) or omit to auto-detect.");
                }
            }


            _logger.LogInformation(
                "Uploading WordSmith dictionary: {Name}, File: {FileName}, Size: {Size} bytes",
                request.Name, request.File.FileName, request.File.Length);

           

            // Create upload DTO
            var uploadDto = new UploadWordSmithDictionaryDto
            {
                Name = request.Name,
                Description = request.Description ?? string.Empty,
                Category = request.Category ?? "Custom"
            };

            // Upload dictionary using the service
            WordSmithDictionaryDto dictionary;
            using (var stream = request.File.OpenReadStream())
            {
                if (request.DictionaryId.HasValue)
                {
                    dictionary = await _service.ReplaceDictionaryAsync(
                      request.DictionaryId.Value,
                      stream,
                      request.File.FileName,
                      uploadDto);

                    _logger.LogInformation("Dictionary {DictionaryId} replaced with new file {FileName}",
                        dictionary.Id, request.File.FileName);
                }
                else
                {
                    dictionary = await _service.UploadDictionaryAsync(
                    stream,
                    request.File.FileName,
                    uploadDto);
                }

            }

            _logger.LogInformation(
                "Dictionary uploaded successfully with ID: {DictionaryId}",
                dictionary.Id);

            var dictionaryPath = await _wordSmithDictionaryRepository.GetByIdAsync(dictionary.Id, Constants.Collections.WordSmithDictionary);
            // Load the dictionary to get rule details for preview
            var loadResult = _loader.LoadDictionary(
                dictionaryPath.OriginalFilePath,
                Encoding.Unicode,
                true);

            if (!loadResult.Success)
            {
                _logger.LogWarning(
                    "Failed to load dictionary for preview. Errors: {Errors}",
                    string.Join(", ", loadResult.ErrorMessages.Take(5)));
            }

            // Get rule statistics
            var allRules = loadResult.ReplacementsDictionary.Values.ToList();
            var replacementRuleCount = allRules.Count(r => !string.IsNullOrEmpty(r.Replacement) && !r.ToDelete);
            var deletionRuleCount = allRules.Count(r => r.ToDelete);
            var newColumnRuleCount = allRules.Count(r => !string.IsNullOrEmpty(r.NewColumnName));

            // Get unique extracted column names
            var extractedColumns = allRules
                .Where(r => !string.IsNullOrEmpty(r.NewColumnName))
                .Select(r => r.NewColumnName)
                .Distinct()
                .OrderBy(c => c)
                .ToList();

            // Create preview of first 10 rules for display
            var previewRules = allRules
                .Take(10)
                .Select(rule => new WordSmithRuleDto
                {
                    Words = rule.Words,
                    Replacement = rule.Replacement,
                    NewColumnName = rule.NewColumnName,
                    ToDelete = rule.ToDelete,
                    Priority = rule.Priority,                   
                })
                .ToList();

            // Create response
            var response = new WordSmithDictionaryResponse
            {
                Id = dictionary.Id,
                Name = dictionary.Name,
                Description = dictionary.Description,
                Category = dictionary.Category,
                OriginalFileName = dictionary.OriginalFileName,
                Version = dictionary.Version,
                TotalRuleCount = allRules.Count,
                ReplacementRuleCount = replacementRuleCount,
                DeletionRuleCount = deletionRuleCount,
                NewColumnRuleCount = newColumnRuleCount,
                ExtractedColumns = extractedColumns,
                CreatedAt = dictionary.CreatedAt,
                PreviewRules = previewRules
            };

            _logger.LogInformation(
                "Dictionary {Name} processed successfully: {TotalRules} total rules, " +
                "{Replacements} replacements, {Deletions} deletions, {NewColumns} new column rules",
                dictionary.Name,
                response.TotalRuleCount,
                response.ReplacementRuleCount,
                response.DeletionRuleCount,
                response.NewColumnRuleCount);

            return Result<WordSmithDictionaryResponse>.Success(response);
        }
        catch (InvalidOperationException ex)
        {
            _logger.LogError(ex, "Invalid operation while uploading dictionary");
            return Result<WordSmithDictionaryResponse>.Error(ex.Message);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Unexpected error while uploading WordSmith dictionary");
            return Result<WordSmithDictionaryResponse>.Error(
                "An error occurred while uploading the dictionary. Please try again.");
        }
    }
}
