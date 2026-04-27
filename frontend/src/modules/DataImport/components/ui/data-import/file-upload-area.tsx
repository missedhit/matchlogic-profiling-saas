import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { useAppSelector } from "@/hooks/use-store";
import { useRouteGuard } from "@/providers/route-guard-provider";
import { BookOpen, FileIcon, LoaderCircleIcon, Upload } from "lucide-react";
import { GeneralCloseIcon } from "@/assets/icons";
import { useRouter } from "next/navigation";
import { useRef, useState } from "react";
import { useUploadFileMutation } from "@/modules/DataImport/hooks/file/upload-file";
import { getFileType } from "@/modules/DataImport/utils/file-type";

export function FileUploadArea() {
  const { selectedProject } = useAppSelector((s) => s.projects);
  const { selectedFileType, uploadedFile } = useAppSelector(
    (s) => s.dataImport
  );
  const { file, setFile } = useRouteGuard();
  const fileInputRef = useRef<HTMLInputElement>(null);
  const router = useRouter();

  const { mutate, isPending, uploadStep } = useUploadFileMutation();
  const [isDragging, setIsDragging] = useState(false);
  const onDrop = (e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    setIsDragging(false);

    if (e.dataTransfer.files && e.dataTransfer.files.length > 0) {
      setFile(e.dataTransfer.files[0]);
    }
  };
  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files.length > 0) {
      setFile(e.target.files[0]);
    }
  };

  const onImport = () => {
    if (!file) return;

    try {
      const projectSelectedId = selectedProject?.id || "";

      if (file && selectedProject?.id) {
        const fileType = getFileType(file.name);
        mutate({
          projectId: projectSelectedId,
          dataSourceType: fileType,
          file,
        });
      }
    } catch (err: any) {
      console.error(err);
    }
  };
  return (
    <div className="w-full">
      <Card className="p-6 border rounded-lg shadow-sm bg-iris-mist">
        <h2 className="text-xl font-semibold mb-4">
          Upload a {selectedFileType.toUpperCase()} file
        </h2>

        <div
          className={`border-2 border-dashed rounded-lg p-10 flex flex-col items-center justify-center h-32 transition-colors ${
            isDragging ? "border-primary bg-primary/10" : "border-primary/30"
          }`}
          onDragOver={(e) => {
            e.preventDefault();
            setIsDragging(true);
          }}
          onDragLeave={() => {
            setIsDragging(false);
          }}
          onDrop={onDrop}
        >
          {file ? (
            <div className="w-full">
              <div className="flex items-center justify-between bg-white rounded-lg p-3 mb-4 border">
                <div className="flex items-center">
                  <div className="bg-primary rounded p-2 mr-3">
                    <FileIcon className="h-5 w-5 text-white" />
                  </div>
                  <span className="font-medium">{file.name}</span>
                </div>
                <Button
                  onClick={() => setFile(null)}
                  variant="ghost"
                  size="icon"
                  className="rounded-full h-8 w-8 hover:bg-gray-100"
                  aria-label="Remove file"
                >
                  <GeneralCloseIcon className="h-4 w-4 text-gray-500" />
                </Button>
              </div>

              {uploadedFile ? (
                <Button
                  className="w-full"
                  onClick={() => router.push(`/data-import/column-mapping`)}
                >
                  Continue Import
                </Button>
              ) : (
                <Button
                  className="w-full"
                  onClick={onImport}
                  disabled={isPending}
                  requiredPermission="dataimport.execute"
                >
                  {isPending ? (
                    <>
                      <LoaderCircleIcon className="animate-spin mr-2" />
                      {uploadStep === "loading-tables"
                        ? "Loading tables..."
                        : "Uploading..."}
                    </>
                  ) : (
                    "Upload"
                  )}
                </Button>
              )}
            </div>
          ) : (
            <>
              <Upload className="h-10 w-10 text-primary mb-4" />
              <p className="text-center mb-4">Drag and drop or</p>
              <Button
                variant="default"
                className=""
                onClick={() => {
                  fileInputRef.current?.click();
                }}
                requiredPermission="dataimport.execute"
              >
                Select Files
              </Button>
              <input
                ref={fileInputRef}
                type="file"
                className="hidden"
                accept={
                  selectedFileType === "csv"
                    ? ".csv"
                    : selectedFileType === "xlsx"
                      ? ".xlsx,.xls"
                      : ""
                }
                onChange={handleFileChange}
              />
            </>
          )}
        </div>

        <div className="mt-4 text-sm text-center">
          <BookOpen aria-hidden="true" className="inline h-4 w-4 mr-1" />
          Learn more about importing contacts or download a sample{" "}
          {selectedFileType.toUpperCase()} file
        </div>
      </Card>
    </div>
  );
}
