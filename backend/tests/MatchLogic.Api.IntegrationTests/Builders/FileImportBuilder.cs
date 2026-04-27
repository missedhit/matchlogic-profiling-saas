using MatchLogic.Application.Interfaces.Import;
using MatchLogic.Domain.Import;
using Microsoft.Extensions.DependencyInjection;
using NPOI.HPSF;
using NPOI.SS.UserModel;
using NPOI.XSSF.UserModel;

namespace MatchLogic.Api.IntegrationTests.Builders;
public class FileImportBuilder
{
    private readonly FileImport _fileImport = new();
    private readonly string _fileTempName = Path.GetRandomFileName() + ".xlsx";
    private readonly string UploadFolderPath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData), "MatchLogicApi", "Uploads");

    private readonly IServiceProvider _serviceProvider;
    public FileImportBuilder(IServiceProvider _serviceProvider)
    {
        this._serviceProvider = _serviceProvider ?? throw new ArgumentNullException(nameof(_serviceProvider));
        Directory.CreateDirectory(UploadFolderPath);


        var fileId = Guid.NewGuid();

        //var project = await new ProjectBuilder(_serviceProvider).BuildAsync();        
        _fileImport.Id = fileId;
        //_fileImport.ProjectId = project.Id;// Set this to a valid/Invalid/Empty project ID
        _fileImport.DataSourceType = DataSourceType.Excel;
        _fileImport.OriginalName = _fileTempName;

        CreateTestExcelFile();
    }
    private FileImportBuilder CreateExcelFile(Action<IWorkbook, ISheet> populateWorkbookAction)
    {
        var fileExtension = ".xlsx";
        var fileName = $"{_fileImport.Id}{fileExtension}";
        var filePath = Path.Combine(UploadFolderPath, fileName);

        using (var workbook = new XSSFWorkbook())
        {
            var sheet = workbook.CreateSheet("Sheet1");
            var sheet2 = workbook.CreateSheet("Sheet2");

            populateWorkbookAction(workbook, sheet);

            using (var fileStream = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.None))
            {
                workbook.Write(fileStream);
            }
        }

        UpdateFileImportMetadata(filePath, fileName, fileExtension);

        return this;
    }

    private void UpdateFileImportMetadata(string filePath, string fileName, string fileExtension)
    {
        _fileImport.OriginalName = _fileTempName;
        _fileImport.FileName = fileName;
        _fileImport.FilePath = filePath;
        _fileImport.FileSize = new FileInfo(filePath).Length;
        _fileImport.FileExtension = fileExtension;
    }

    public FileImportBuilder CreateTestExcelFile()
    {
        return CreateExcelFile((workbook, sheet) => {
            // Create header row
            var headerRow = sheet.CreateRow(0);
            headerRow.CreateCell(0).SetCellValue("Name");
            headerRow.CreateCell(1).SetCellValue("Age");

            // Add test data
            var row1 = sheet.CreateRow(1);
            row1.CreateCell(0).SetCellValue("John Doe");
            row1.CreateCell(1).SetCellValue(30);

            var row2 = sheet.CreateRow(2);
            row2.CreateCell(0).SetCellValue("Jane Smith");
            row2.CreateCell(1).SetCellValue(25);
        });
    }

    public FileImportBuilder CreateDuplicateTestFile()
    {
        return CreateExcelFile((workbook, sheet) => {
            // Create header row
            var headerRow = sheet.CreateRow(0);
            headerRow.CreateCell(0).SetCellValue("Name");
            headerRow.CreateCell(1).SetCellValue("Age");
            headerRow.CreateCell(2).SetCellValue("Name");
            headerRow.CreateCell(3).SetCellValue("Age");

            // Add test data
            var row1 = sheet.CreateRow(1);
            row1.CreateCell(0).SetCellValue("John Doe");
            row1.CreateCell(1).SetCellValue(30);
            row1.CreateCell(2).SetCellValue("Brett Pierce");
            row1.CreateCell(3).SetCellValue(40);

            var row2 = sheet.CreateRow(2);
            row2.CreateCell(0).SetCellValue("Jane Smith");
            row2.CreateCell(1).SetCellValue(25);
            row2.CreateCell(2).SetCellValue("Byran Dumphries");
            row2.CreateCell(3).SetCellValue(28);
        });
    }

    public FileImportBuilder CreateValidColumnNamesTestExcelFile()
    {
        return CreateExcelFile((workbook, sheet) => {
            // Create header row
            var headerRow = sheet.CreateRow(0);
            headerRow.CreateCell(0).SetCellValue("Name");
            headerRow.CreateCell(1).SetCellValue("_Name");
            headerRow.CreateCell(2).SetCellValue("Name_");
            headerRow.CreateCell(3).SetCellValue("2222");
            headerRow.CreateCell(4).SetCellValue("2Name");
            headerRow.CreateCell(5).SetCellValue("Name2");
            headerRow.CreateCell(6).SetCellValue("Full Name");
            headerRow.CreateCell(7).SetCellValue("Age");
            headerRow.CreateCell(8).SetCellValue("Zip");

            // Add test data
            var row1 = sheet.CreateRow(1);
            row1.CreateCell(0).SetCellValue("John Doe");
            row1.CreateCell(1).SetCellValue("Aryn Boyne");
            row1.CreateCell(2).SetCellValue("Mickey Pasley");
            row1.CreateCell(3).SetCellValue("Abagail Manuel");
            row1.CreateCell(4).SetCellValue("Colene McClean");
            row1.CreateCell(5).SetCellValue("Brett Pierce");
            row1.CreateCell(6).SetCellValue("Sofia Allner");
            row1.CreateCell(7).SetCellValue(30);
            row1.CreateCell(8).SetCellValue("84791");

            var row2 = sheet.CreateRow(2);
            row2.CreateCell(0).SetCellValue("Jane Smith");
            row2.CreateCell(1).SetCellValue("Kala Clifforth");
            row2.CreateCell(2).SetCellValue("Jud Billows");
            row2.CreateCell(3).SetCellValue("Worth Truder");
            row2.CreateCell(4).SetCellValue("Danell Huntingford");
            row2.CreateCell(5).SetCellValue("Byran Dumphries");
            row2.CreateCell(6).SetCellValue("Filippa Hadleigh");
            row2.CreateCell(7).SetCellValue(25);
            row2.CreateCell(8).SetCellValue("84792");
        });
    }


    public FileImportBuilder WithProjectId(Guid projectId)
    {
        _fileImport.ProjectId = projectId;
        return this;
    }

    public FileImportBuilder WithFileName(string fileName)
    {
        if (string.IsNullOrWhiteSpace(fileName))
            throw new ArgumentException("FileName cannot be null or empty.", nameof(fileName));

        _fileImport.FileName = fileName;
        return this;
    }

    public FileImportBuilder WithOriginalName(string originalName)
    {
        if (string.IsNullOrWhiteSpace(originalName))
            throw new ArgumentException("OriginalName cannot be null or empty.", nameof(originalName));

        _fileImport.OriginalName = originalName;
        return this;
    }

    public FileImportBuilder WithFilePath(string filePath)
    {
        if (string.IsNullOrWhiteSpace(filePath))
            throw new ArgumentException("FilePath cannot be null or empty.", nameof(filePath));

        var fileExt = Path.GetExtension(filePath);
        var fileName = $"{_fileImport.Id}{fileExt}";
        var destinationFilePath = Path.Combine(UploadFolderPath, fileName);

        // Copy the test Excel file to the upload folder
        File.Copy(filePath, destinationFilePath, overwrite: true);

        //Set the File Metadata
        _fileImport.OriginalName = Path.GetFileName(filePath);//Orignal File Name
        _fileImport.FileName = fileName;
        _fileImport.FilePath = destinationFilePath;
        _fileImport.FileSize = new FileInfo(destinationFilePath).Length;
        _fileImport.FileExtension = fileExt;
        return this;
    }

    public FileImportBuilder WithFileSize(long fileSize)
    {
        if (fileSize <= 0)
            throw new ArgumentException("FileSize must be greater than zero.", nameof(fileSize));

        _fileImport.FileSize = fileSize;
        return this;
    }

    public FileImportBuilder WithFileExtension(string fileExtension)
    {
        if (string.IsNullOrWhiteSpace(fileExtension))
            throw new ArgumentException("FileExtension cannot be null or empty.", nameof(fileExtension));

        _fileImport.FileExtension = fileExtension;
        return this;
    }

    public FileImportBuilder WithDataSourceType(DataSourceType dataSourceType)
    {
        _fileImport.DataSourceType = dataSourceType;
        return this;
    }

    public FileImport BuildDomain()
    {
        if ( _fileImport.ProjectId == Guid.Empty)
            throw new InvalidOperationException("ProjectId must be set before creating the FileImport.");

        if (string.IsNullOrWhiteSpace(_fileImport.FilePath))
            throw new ArgumentException("FilePath cannot be null or empty.", nameof(_fileImport.FilePath));

        return _fileImport;
    }
    /*public async Task<FileImport> CreateValidDomainAsync()
    {
        var fileId = Guid.NewGuid();
        var fileExtension = Path.GetExtension(_fileTempName);
        var fileName = $"{fileId}{fileExtension}";
        var filePath = Path.Combine(UploadFolderPath, fileName);

        // Copy the test Excel file to the upload folder
        System.IO.File.Copy(_testTempExcelPath, filePath, overwrite: true);


        var project = await new ProjectBuilder(_serviceProvider).BuildAsync();
        _projectId = project.Id;

        _fileImport.Id = fileId;
        _fileImport.ProjectId = _projectId;
        _fileImport.DataSourceType = DataSourceType.Excel;
        _fileImport.OriginalName = _fileTempName;
        _fileImport.FileName = fileName;
        _fileImport.FilePath = filePath;
        _fileImport.FileSize = new FileInfo(filePath).Length;
        _fileImport.FileExtension = fileExtension;     
        var fileImportService = this._serviceProvider.GetRequiredService<IFileImportService>();
        if (_projectId == Guid.Empty)
            throw new InvalidOperationException("ProjectId must be set before creating the FileImport.");
        // Use the service to create the FileImport (assuming such a method exists)
        var createdFileImport = await fileImportService.CreateFile(_fileImport);
        return createdFileImport;
    }*/

    public async Task<FileImport> BuildAsync()
    {
        var fileImportService = this._serviceProvider.GetRequiredService<IFileImportService>();

        BuildDomain();

        // Use the service to create the FileImport (assuming such a method exists)
        var createdFileImport = await fileImportService.CreateFile(_fileImport);
        return createdFileImport;
    }
}
