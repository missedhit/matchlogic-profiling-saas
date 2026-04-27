export const DONE_PREFIX = "[DONE] ";

export function isDone(note: string | undefined | null): boolean {
	return !!note && note.startsWith(DONE_PREFIX);
}

export function stripDone(note: string | undefined | null): string {
	if (!note) return "";
	return note.startsWith(DONE_PREFIX) ? note.slice(DONE_PREFIX.length) : note;
}

export function applyDoneStatus(note: string, done: boolean): string {
	const base = stripDone(note);
	return done ? `${DONE_PREFIX}${base}` : base;
}
