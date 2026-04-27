import { TabsContent } from "@/components/ui/tabs";
import { GeneralWarningIcon, GeneralSuccessIcon } from "@/assets/icons";

export interface Exception {
	tableName: string;
	message: string;
}

export function ExceptionTab({ exceptions }: { exceptions: Exception[] }) {
	return (
		<TabsContent value="exception">
			<div className="border rounded-md">
				<div className="max-h-[400px] overflow-auto custom-scrollbar p-4">
					{exceptions.length > 0 ? (
						<div className="space-y-4">
							{exceptions.map((exception, index) => (
								<div
									key={index}
									className="p-4 border border-yellow-300 bg-yellow-50 rounded-md"
								>
									<div className="flex items-start">
										<GeneralWarningIcon className="h-5 w-5 text-yellow-500 mr-2 mt-0.5" />
										<div>
											<p className="font-medium">{exception.message}</p>
										</div>
									</div>
								</div>
							))}
						</div>
					) : (
						<div className="p-8 text-center text-muted-foreground">
							<GeneralSuccessIcon className="h-12 w-12 mx-auto mb-4 text-green-500" />
							<h3 className="text-lg font-medium mb-2">No exceptions found</h3>
							<p>All data in the selected files appears to be valid.</p>
						</div>
					)}
				</div>
			</div>
		</TabsContent>
	);
}
