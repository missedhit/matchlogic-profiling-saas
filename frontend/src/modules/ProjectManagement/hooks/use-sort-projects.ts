import { useEffect, useState } from "react";
import { useAppSelector } from "@/hooks/use-store";
import { useProjectsList } from "../hooks/use-projects-list";
import { Project } from "@/models/api-responses";

export default function useSortProjects(projectsPerPage = 10) {
	const { data, isLoading } = useProjectsList();
	const projects = data?.value;
	const [sortedProjects, setSortedProjects] = useState<Project[]>([]);
	const [filteredProjects, setFilteredProjects] = useState<Project[]>([]);
	const [currentPage, setCurrentPage] = useState(1);
	const [totalPages, setTotalPages] = useState(1);
	const { searchTerm, sortConfig } = useAppSelector((state) => state.projects);
	useEffect(() => {
		if (projects) {
			const filteredProjects = projects?.filter((project: Project) => {
				const matchesSearch =
					project?.name?.toLowerCase().includes(searchTerm.toLowerCase()) ||
					project?.description
						?.toLowerCase()
						.includes(searchTerm.toLowerCase());
				return matchesSearch;
			});
			const sortedProjects = [...filteredProjects].sort((a, b) => {
				if (!sortConfig.key) return 0;
				const aValue = new Date(a[sortConfig.key]).getTime();
				const bValue = new Date(b[sortConfig.key]).getTime();
				if (sortConfig.direction === "asc") {
					return aValue - bValue;
				} else {
					return bValue - aValue;
				}
			});
			setSortedProjects(sortedProjects);
		}
	}, [projects, sortConfig, searchTerm]);

	useEffect(() => {
		// Calculate pagination
		const pageCount = Math.ceil(sortedProjects.length / projectsPerPage);

		setTotalPages(pageCount || 1);
		const startIndex = (currentPage - 1) * projectsPerPage;
		const endIndex = startIndex + projectsPerPage;
		const currentProjects = sortedProjects.slice(startIndex, endIndex);

		setFilteredProjects(currentProjects);
	}, [sortedProjects, currentPage]);

	useEffect(() => {
		if (currentPage > totalPages) {
			setCurrentPage(totalPages);
		}
	}, [totalPages, currentPage]);

	return {
		filteredProjects,
		totalPages,
		currentPage,
		setCurrentPage,
	};
}
