"use client";

import { useState, useEffect } from "react";
import {
	Dialog,
	DialogContent,
	DialogHeader,
	DialogTitle,
	DialogFooter,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
	Table,
	TableBody,
	TableCell,
	TableHead,
	TableHeader,
	TableRow,
} from "@/components/ui/table";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { useRegexPatternsQuery } from "@/modules/DataProfiling/hooks/use-regex-patterns-query";
import { useCreateRegexPattern } from "@/modules/DataProfiling/hooks/use-create-regex-pattern";
import { useUpdateRegexPattern } from "@/modules/DataProfiling/hooks/use-update-regex-pattern";
import { useDeleteRegexPattern } from "@/modules/DataProfiling/hooks/use-delete-regex-pattern";
import { Pencil, Check, Trash2 } from "lucide-react";
import { GeneralCloseIcon } from "@/assets/icons";
import {
	DataProfilingAddNewPatternIcon,
	DataProfilingCustomIcon,
	DataProfilingDefaultIcon,
} from "@/assets/icons";
import {
	AlertDialog,
	AlertDialogAction,
	AlertDialogCancel,
	AlertDialogContent,
	AlertDialogDescription,
	AlertDialogFooter,
	AlertDialogHeader,
	AlertDialogTitle,
} from "@/components/ui/alert-dialog";

interface Pattern {
	id: string;
	name: string;
	type: string;
	pattern: string;
	selected: boolean;
	description?: string;
	isDefault: boolean;
	originalIsDefault: boolean; // Track original value to detect changes
}

interface PatternOptionsModalProps {
	open: boolean;
	onOpenChange: (open: boolean) => void;
	onGenerate?: () => void;
	isGenerating?: boolean;
}

export function PatternOptionsModal({
	open,
	onOpenChange,
	onGenerate,
	isGenerating,
}: PatternOptionsModalProps) {
	const { data: regexPatternsData, isLoading: isLoadingPatterns } =
		useRegexPatternsQuery();
	const createPatternMutation = useCreateRegexPattern();
	const updatePatternMutation = useUpdateRegexPattern();
	const deletePatternMutation = useDeleteRegexPattern();

	const [defaultPatternsState, setDefaultPatternsState] = useState<Pattern[]>(
		[]
	);
	const [customPatternsState, setCustomPatternsState] = useState<Pattern[]>([]);
	const [activeTab, setActiveTab] = useState("default");

	// Form state for Add New Pattern
	const [newPatternName, setNewPatternName] = useState("");
	const [newPatternType, setNewPatternType] = useState("");
	const [newPatternPattern, setNewPatternPattern] = useState("");

	// Edit state for Custom Patterns
	const [editingPatternId, setEditingPatternId] = useState<string | null>(null);
	const [editedName, setEditedName] = useState("");
	const [editedType, setEditedType] = useState("");
	const [editedPattern, setEditedPattern] = useState("");

	// Delete confirmation state
	const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
	const [patternToDelete, setPatternToDelete] = useState<Pattern | null>(null);

	// Load and filter patterns from API
	useEffect(() => {
		if (regexPatternsData?.value) {
			const defaultPatterns: Pattern[] = [];
			const customPatterns: Pattern[] = [];

			regexPatternsData.value.forEach((item) => {
				const pattern: Pattern = {
					id: item.id,
					name: item.name,
					type: item.description || "Pattern",
					pattern: item.regexExpression,
					selected: item.isDefault, // Use isDefault for checkbox state
					description: item.description,
					isDefault: item.isDefault,
					originalIsDefault: item.isDefault, // Track original value
				};

				// Only isSystemDefault patterns go to default section
				if (item.isSystemDefault) {
					defaultPatterns.push(pattern);
				} else {
					// Everything else is a custom pattern
					customPatterns.push(pattern);
				}
			});

			setDefaultPatternsState(defaultPatterns);
			setCustomPatternsState(customPatterns);
		}
	}, [regexPatternsData]);

	const handleSelectPatternCustom = (id: string, checked: boolean) => {
		setCustomPatternsState(
			customPatternsState.map((p) =>
				p.id === id ? { ...p, selected: checked, isDefault: checked } : p
			)
		);
	};

	const handleEditPattern = (pattern: Pattern) => {
		setEditingPatternId(pattern.id);
		setEditedName(pattern.name);
		setEditedType(pattern.type);
		setEditedPattern(pattern.pattern);
	};

	const handleCancelEdit = () => {
		setEditingPatternId(null);
		setEditedName("");
		setEditedType("");
		setEditedPattern("");
	};

	const handleSaveEdit = async () => {
		if (
			!editingPatternId ||
			!editedName.trim() ||
			!editedType.trim() ||
			!editedPattern.trim()
		) {
			return;
		}

		const currentPattern = customPatternsState.find(
			(p) => p.id === editingPatternId
		);
		try {
			await updatePatternMutation.mutateAsync({
				id: editingPatternId,
				name: editedName.trim(),
				description: editedType.trim(),
				regexExpression: editedPattern.trim(),
				isDefault: currentPattern?.isDefault ?? false,
			});

			// Clear edit state
			handleCancelEdit();
		} catch (error) {
			console.error("Failed to update pattern:", error);
		}
	};

	const handleDeleteClick = (pattern: Pattern) => {
		setPatternToDelete(pattern);
		setDeleteDialogOpen(true);
	};

	const handleConfirmDelete = async () => {
		if (!patternToDelete) return;

		try {
			await deletePatternMutation.mutateAsync(patternToDelete.id);
			setDeleteDialogOpen(false);
			setPatternToDelete(null);
		} catch (error) {
			console.error("Failed to delete pattern:", error);
		}
	};

	const handleCancelDelete = () => {
		setDeleteDialogOpen(false);
		setPatternToDelete(null);
	};

	const handleAddNewPattern = async () => {
		// Validate form
		if (!newPatternName.trim() || !newPatternPattern.trim()) {
			return;
		}

		try {
			await createPatternMutation.mutateAsync({
				name: newPatternName.trim(),
				description: newPatternType.trim(),
				regexExpression: newPatternPattern.trim(),
				isDefault: true,
			});

			// Clear form
			setNewPatternName("");
			setNewPatternType("");
			setNewPatternPattern("");

			// Switch to custom tab to show the newly added pattern
			setActiveTab("custom");
		} catch (error) {
			console.error("Failed to create pattern:", error);
		}
	};

	const isAddPatternFormValid =
		newPatternName.trim() && newPatternPattern.trim();

	return (
		<>
			<Dialog open={open} onOpenChange={onOpenChange}>
				<DialogContent
					className="!max-w-[1000px] w-[80vw] h-[600px] flex flex-col p-6"
				>
					<DialogHeader className="pb-4">
						<DialogTitle className="text-xl font-semibold">
							Pattern Options
						</DialogTitle>
						<p className="text-sm text-gray-500 mt-2">
							Manage the regex patterns used to classify and validate field data
							during profiling.
						</p>
					</DialogHeader>

					<Tabs
						value={activeTab}
						onValueChange={setActiveTab}
						className="flex-1 flex flex-col min-h-0"
					>
						<TabsList className="grid w-fit grid-cols-3 gap-4 mb-4 bg-transparent border-none h-auto p-0">
							<TabsTrigger
								value="default"
								className="flex items-center gap-2 rounded-none border-b-2 px-4 py-2 data-[state=active]:border-b-primary data-[state=active]:bg-transparent data-[state=inactive]:border-b-border"
							>
								<DataProfilingDefaultIcon className="w-4 h-4" />
								Default
							</TabsTrigger>
							<TabsTrigger
								value="custom"
								className="flex items-center gap-2 rounded-none border-b-2 px-4 py-2 data-[state=active]:border-b-primary data-[state=active]:bg-transparent data-[state=inactive]:border-b-border"
							>
								<DataProfilingCustomIcon className="w-4 h-4" />
								Custom
							</TabsTrigger>
							<TabsTrigger
								value="add"
								className="flex items-center gap-2 rounded-none border-b-2 px-4 py-2 data-[state=active]:border-b-primary data-[state=active]:bg-transparent data-[state=inactive]:border-b-border"
							>
								<DataProfilingAddNewPatternIcon className="w-4 h-4" />
								Add New Pattern
							</TabsTrigger>
						</TabsList>

						<TabsContent value="default" className="flex-1 overflow-auto mt-4">
							{isLoadingPatterns ? (
								<div className="flex items-center justify-center h-full min-h-[300px]">
									<div className="text-gray-500">Loading patterns...</div>
								</div>
							) : (
								<div className="border rounded-md overflow-hidden">
									<Table>
										<TableHeader className="sticky top-0 bg-white z-10 border-b">
											<TableRow className="hover:bg-transparent">
												<TableHead className="w-[120px] bg-white">
													<span>Selected</span>
												</TableHead>
												<TableHead className="w-[200px] bg-white">
													Name
												</TableHead>
												<TableHead className="w-[150px] bg-white">
													Description
												</TableHead>
												<TableHead className="bg-white">Pattern</TableHead>
											</TableRow>
										</TableHeader>
										<TableBody>
											{defaultPatternsState.length === 0 ? (
												<TableRow>
													<TableCell
														colSpan={4}
														className="text-center text-gray-500 py-8"
													>
														No default patterns available
													</TableCell>
												</TableRow>
											) : (
												defaultPatternsState.map((pattern) => (
													<TableRow key={pattern.id}>
														<TableCell>
															<Checkbox
																checked={pattern.selected}
																disabled
																className="data-[state=checked]:bg-primary data-[state=checked]:border-primary"
															/>
														</TableCell>
														<TableCell className="font-medium">
															{pattern.name}
														</TableCell>
														<TableCell>{pattern.type}</TableCell>
														<TableCell className="font-mono text-xs text-gray-600 max-w-md truncate">
															{pattern.pattern}
														</TableCell>
													</TableRow>
												))
											)}
										</TableBody>
									</Table>
								</div>
							)}
						</TabsContent>

						<TabsContent value="custom" className="flex-1 overflow-auto mt-4">
							<div className="border rounded-md overflow-hidden">
								<Table>
									<TableHeader className="sticky top-0 bg-white z-10 border-b">
										<TableRow className="hover:bg-transparent">
											<TableHead className="w-[120px] bg-white">
												<span>Selected</span>
											</TableHead>
											<TableHead className="w-[200px] bg-white">Name</TableHead>
											<TableHead className="w-[150px] max-w-[150px] bg-white">
												Description
											</TableHead>
											<TableHead className="bg-white">Pattern</TableHead>
											<TableHead className="w-[100px] bg-white">
												Actions
											</TableHead>
										</TableRow>
									</TableHeader>
									<TableBody>
										{customPatternsState.length === 0 ? (
											<TableRow>
												<TableCell
													colSpan={5}
													className="text-center text-gray-500 py-8"
												>
													No custom patterns available. Create one in the "Add
													New Pattern" tab.
												</TableCell>
											</TableRow>
										) : (
											customPatternsState.map((pattern) => {
												const isEditing = editingPatternId === pattern.id;
												return (
													<TableRow key={pattern.id}>
														<TableCell>
															<Checkbox
																checked={pattern.selected}
																onCheckedChange={(checked) =>
																	handleSelectPatternCustom(
																		pattern.id,
																		checked as boolean
																	)
																}
																className="data-[state=checked]:bg-primary data-[state=checked]:border-primary"
																disabled={isEditing}
															/>
														</TableCell>
														<TableCell className="font-medium">
															{isEditing ? (
																<Input
																	value={editedName}
																	onChange={(e) =>
																		setEditedName(e.target.value)
																	}
																	className="h-8"
																/>
															) : (
																pattern.name
															)}
														</TableCell>
														<TableCell className="max-w-[150px] text-ellipsis whitespace-nowrap overflow-hidden">
															{isEditing ? (
																<Input
																	value={editedType}
																	onChange={(e) =>
																		setEditedType(e.target.value)
																	}
																	className="h-8"
																/>
															) : (
																pattern.type
															)}
														</TableCell>
														<TableCell className="font-mono text-xs text-gray-600">
															{isEditing ? (
																<Input
																	value={editedPattern}
																	onChange={(e) =>
																		setEditedPattern(e.target.value)
																	}
																	className="h-8 font-mono text-xs"
																/>
															) : (
																<span className="truncate max-w-md block">
																	{pattern.pattern}
																</span>
															)}
														</TableCell>
														<TableCell>
															{isEditing ? (
																<div className="flex items-center gap-2">
																	<Button
																		size="sm"
																		variant="ghost"
																		onClick={handleSaveEdit}
																		disabled={updatePatternMutation.isPending}
																		className="h-8 w-8 p-0"
																	>
																		<Check className="h-4 w-4 text-green-600" />
																	</Button>
																	<Button
																		size="sm"
																		variant="ghost"
																		onClick={handleCancelEdit}
																		disabled={updatePatternMutation.isPending}
																		className="h-8 w-8 p-0"
																	>
																		<GeneralCloseIcon className="h-4 w-4 text-red-600" />
																	</Button>
																</div>
															) : (
																<div className="flex items-center gap-2">
																	<Button
																		size="sm"
																		variant="ghost"
																		onClick={() => handleEditPattern(pattern)}
																		className="h-8 w-8 p-0"
																	>
																		<Pencil className="h-4 w-4 text-gray-600" />
																	</Button>
																	<Button
																		size="sm"
																		variant="ghost"
																		onClick={() => handleDeleteClick(pattern)}
																		className="h-8 w-8 p-0"
																	>
																		<Trash2 className="h-4 w-4 text-red-600" />
																	</Button>
																</div>
															)}
														</TableCell>
													</TableRow>
												);
											})
										)}
									</TableBody>
								</Table>
							</div>
						</TabsContent>

						<TabsContent value="add" className="flex-1 mt-4">
							<div className="space-y-6 max-w-2xl">
								<div className="space-y-2">
									<Label htmlFor="pattern-name">Name</Label>
									<Input
										id="pattern-name"
										value={newPatternName}
										onChange={(e) => setNewPatternName(e.target.value)}
										placeholder="Enter pattern name"
									/>
								</div>

								<div className="space-y-2">
									<Label htmlFor="pattern-type">Description</Label>
									<Input
										id="pattern-type"
										value={newPatternType}
										onChange={(e) => setNewPatternType(e.target.value)}
										placeholder="Enter pattern description"
									/>
								</div>

								<div className="space-y-2">
									<Label htmlFor="pattern-regex">Pattern</Label>
									<Input
										id="pattern-regex"
										value={newPatternPattern}
										onChange={(e) => setNewPatternPattern(e.target.value)}
										placeholder="Enter regex pattern"
										className="font-mono text-xs"
									/>
								</div>

								<div className="flex justify-end pt-4">
									<Button
										onClick={handleAddNewPattern}
										disabled={
											!isAddPatternFormValid || createPatternMutation.isPending
										}
									>
										{createPatternMutation.isPending
											? "Adding..."
											: "Add Pattern"}
									</Button>
								</div>
							</div>
						</TabsContent>
					</Tabs>

					<DialogFooter className="mt-4 pt-4 border-t">
						<Button variant="outline" onClick={() => onOpenChange(false)}>
							Close
						</Button>
						{onGenerate && (
							<Button
								onClick={() => {
									onGenerate();
									onOpenChange(false);
								}}
								disabled={isGenerating}
								disabledReason="Profiling job in progress"
								requiredPermission="profiling.execute"
							>
								{isGenerating ? "Generating..." : "Generate"}
							</Button>
						)}
					</DialogFooter>
				</DialogContent>
			</Dialog>

			<AlertDialog open={deleteDialogOpen} onOpenChange={setDeleteDialogOpen}>
				<AlertDialogContent>
					<AlertDialogHeader>
						<AlertDialogTitle>Delete Pattern</AlertDialogTitle>
						<AlertDialogDescription>
							Are you sure you want to delete the pattern "
							{patternToDelete?.name}"? This action cannot be undone.
						</AlertDialogDescription>
					</AlertDialogHeader>
					<AlertDialogFooter>
						<AlertDialogCancel
							onClick={handleCancelDelete}
							disabled={deletePatternMutation.isPending}
						>
							Cancel
						</AlertDialogCancel>
						<AlertDialogAction
							onClick={handleConfirmDelete}
							disabled={deletePatternMutation.isPending}
							className="bg-red-600 hover:bg-red-700"
						>
							{deletePatternMutation.isPending ? "Deleting..." : "Delete"}
						</AlertDialogAction>
					</AlertDialogFooter>
				</AlertDialogContent>
			</AlertDialog>
		</>
	);
}
