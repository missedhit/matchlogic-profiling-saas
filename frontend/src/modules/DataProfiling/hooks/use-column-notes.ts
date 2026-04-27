import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { store } from "@/store";
import { apiFetch } from "@/utils/apiFetch";

export interface ColumnNotesResponse {
	id: string;
	dataSourceId: string;
	columnNotes: Record<string, string>;
}

const columnNotesKey = (dataSourceId: string | undefined) => [
	"column-notes",
	dataSourceId ?? "",
];

export function useColumnNotesQuery(dataSourceId: string | undefined) {
	return useQuery({
		queryKey: columnNotesKey(dataSourceId),
		queryFn: async (): Promise<ColumnNotesResponse | null> => {
			const response = await apiFetch<ColumnNotesResponse>(
				store.dispatch,
				store.getState,
				`/ColumnNotes?dataSourceId=${dataSourceId}`,
				{
					method: "GET",
					headers: { "X-Skip-Toast": "true", "X-Skip-Loader": "true" },
				}
			);
			return (response as any)?.value ?? null;
		},
		enabled: !!dataSourceId,
		select: (data): Record<string, string> => data?.columnNotes ?? {},
	});
}

export function useSaveColumnNotesMutation(dataSourceId: string | undefined) {
	const queryClient = useQueryClient();

	return useMutation({
		mutationFn: async (columnNotes: Record<string, string>) => {
			const response = await apiFetch<ColumnNotesResponse>(
				store.dispatch,
				store.getState,
				`/ColumnNotes?dataSourceId=${dataSourceId}`,
				{
					method: "POST",
					headers: {
						"Content-Type": "application/json",
						"X-Skip-Loader": "true",
						"X-Skip-Toast": "true",
					},
					body: JSON.stringify({ columnNotes }),
				}
			);
			return (response as any)?.value ?? null;
		},
		onSuccess: () => {
			queryClient.invalidateQueries({ queryKey: columnNotesKey(dataSourceId) });
		},
	});
}
