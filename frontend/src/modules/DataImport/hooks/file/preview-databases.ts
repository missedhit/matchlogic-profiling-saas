import { PreviewDatabaseReponse, SupportedData } from "@/models/api-responses";
import { BasicResponse } from "@/models/basic-response";
import { store } from "@/store";
import { apiFetch } from "@/utils/apiFetch";
import { useMutation, useQuery } from "@tanstack/react-query";
import { useAppSelector } from "@/hooks/use-store";



export const useDatabaseMutation = () => {
	const BASE_PATH = `/dataimport/Preview/Databases`;

	const tablesQuery = useMutation({
		mutationFn: async ({ type, parameters }: { type: keyof typeof SupportedData, parameters: any }) => {
			const url = `${BASE_PATH}`;
			const payload = {
				type: SupportedData[type!],
				parameters: {
					Server: parameters.hostname,
					...(parameters.port && { Port: `${parameters.port}` }),
					...(parameters.auth_type !== "Windows" && {
						Username: parameters.username,
						Password: parameters.password,
					}),
					TrustServerCertificate: parameters.trust_server_certificate ? "true" : "false",
					ConnectionTimeout: `${parameters.timeout || 30}`,
					AuthType: parameters.auth_type
				}

			};
			const response = await apiFetch<PreviewDatabaseReponse>(
				store.dispatch,
				store.getState,
				url,
				{
					method: "POST",
					body: JSON.stringify(payload),
					headers: {
						"Content-Type": "application/json",
						"X-Skip-Success-Toast": "true",
					},
				}
			);
			if (!response?.isSuccess) throw new Error("Failed to list databases");
			return response;
		},
	});
	return tablesQuery;
};
