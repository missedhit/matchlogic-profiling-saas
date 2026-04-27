const MINUTE = 60;
const HOUR = 3600;
const DAY = 86400;

export function getRelativeTime(date: Date | string): string {
	const now = Date.now();
	const then = new Date(date).getTime();
	const diff = Math.floor((now - then) / 1000);

	if (diff < MINUTE) return "just now";
	if (diff < HOUR) {
		const m = Math.floor(diff / MINUTE);
		return `${m}m ago`;
	}
	if (diff < DAY) {
		const h = Math.floor(diff / HOUR);
		return `${h}h ago`;
	}
	if (diff < DAY * 30) {
		const d = Math.floor(diff / DAY);
		return `${d}d ago`;
	}
	return new Date(date).toLocaleDateString("en-US", {
		month: "short",
		day: "numeric",
	});
}
