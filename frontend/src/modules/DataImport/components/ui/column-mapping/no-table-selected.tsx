import { GeneralWarningIcon } from "@/assets/icons";

export function NoTableSelected() {
	return (
		<div className="p-4 bg-red-50 border border-red-200 rounded-md">
			<GeneralWarningIcon className="h-5 w-5 text-red-500 inline-block mr-2" />
			<span className="text-red-600">
				Please select a table to view columns and preview data.
			</span>
		</div>
	);
}
