export const getDataSourceColumns = (data: any) => {
	if (!data || typeof data !== "object" || Array.isArray(data)) return [];
	const allKeys = Object.keys(data);
	return allKeys.filter((key) => key !== "_id" && key !== "_metadata");
};
