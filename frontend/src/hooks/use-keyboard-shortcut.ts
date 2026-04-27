import { useEffect, useCallback } from "react";

type KeyboardShortcutCallback = (event: KeyboardEvent) => void;

export const useKeyboardShortcut = (
	keys: string[],
	callback: KeyboardShortcutCallback
) => {
	// eslint-disable-next-line react-hooks/exhaustive-deps
	const keysKey = JSON.stringify(keys);

	const handleKeyDown = useCallback(
		(event: KeyboardEvent) => {
			const pressedKeys: string[] = [];
			if (event.ctrlKey) pressedKeys.push("Control");
			if (event.shiftKey) pressedKeys.push("Shift");
			if (event.altKey) pressedKeys.push("Alt");
			pressedKeys.push(event.key);

			// Check if all required keys are pressed
			const allKeysMatch = keys.every((key) => pressedKeys.includes(key));

			if (allKeysMatch) {
				event.preventDefault(); // Prevent default browser behavior (e.g., Ctrl+S saving)
				callback(event);
			}
		},
		// Use JSON.stringify(keys) to avoid re-registering on every render when callers pass inline arrays
		// eslint-disable-next-line react-hooks/exhaustive-deps
		[keysKey, callback]
	);

	useEffect(() => {
		window.addEventListener("keydown", handleKeyDown);
		return () => {
			window.removeEventListener("keydown", handleKeyDown);
		};
	}, [handleKeyDown]);
};
