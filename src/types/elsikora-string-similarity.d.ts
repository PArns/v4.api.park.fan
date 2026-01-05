declare module "@elsikora/string-similarity" {
  /**
   * Compares two strings and returns a similarity score between 0 and 1
   * using the Sørensen–Dice coefficient.
   */
  export function compareTwoStrings(str1: string, str2: string): number;
}
