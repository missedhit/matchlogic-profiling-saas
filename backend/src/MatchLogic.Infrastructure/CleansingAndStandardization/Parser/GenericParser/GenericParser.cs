using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.CleansingAndStandardization.Parser.GenericParser
{
    public abstract class GenericParser
    {
        #region Enums

        internal enum PreferedOccurrence
        {
            First,
            Last
        }

        internal enum PreferedWidth
        {
            Min,
            Max
        }

        #endregion

        #region Fields

        private Dictionary<int, bool> createdCategoryIndexes = new Dictionary<int, bool>();

        internal InputWordsList inputWords = new InputWordsList();

        internal PatternsGroupList patternsGroupList = new PatternsGroupList();

        /// <summary>
        /// to prevent checking the same patterns more than once...
        /// </summary>
        private Dictionary<Pattern, Dictionary<int, bool>> alreadyCheckedPatterns =
            new Dictionary<Pattern, Dictionary<int, bool>>();

        internal readonly RecognizedPatterns RecognizedPatterns = new RecognizedPatterns();

        internal ChoosenPatternList ChoosenPatternList;

        #endregion

        #region Constructors

        public GenericParser()
        {
            initPatterns();
        }

        #endregion

        #region Properties and Fields

        private int maxWordsInCategory = 1;

        public int MaxWordsInCategory
        {
            get { return maxWordsInCategory; }
            set { maxWordsInCategory = value; }
        }

        private CategorizedGroupOfWordsList groupOfWordsList = new CategorizedGroupOfWordsList();

        /// <summary>
        /// every time when input string is parsed this group is populated from that input string...
        /// </summary>
        internal CategorizedGroupOfWordsList GroupOfWordsList
        {
            get { return groupOfWordsList; }
            set { groupOfWordsList = value; }
        }

        #endregion

        #region Methods

        internal bool isPatternNullOrRemoved(ChoosenPattern choosenPattern)
        {
            bool result = false;
            if (choosenPattern == null)
            {
                result = true;
            }
            else
            {
                result = choosenPattern.Removed;
            }

            return result;
        }

        protected abstract void initPatterns();

        protected abstract void tryToDetermineUnrecognizedParts();

        /// <summary>
        /// the purpose of this method is to warn if the category with same name is already created
        /// </summary>
        /// <param name="categoryIndex"></param>
        /// <returns></returns>
        internal AbbreviationCategory CreateDictionaryCategory(int categoryIndex)
        {
            AbbreviationCategory result = null;
            if (!createdCategoryIndexes.ContainsKey(categoryIndex))
            {
                createdCategoryIndexes.Add(categoryIndex, false);
                result = new AbbreviationCategory(categoryIndex);
            }

            return result;
        }

        protected abstract Dictionary<int, bool> findCategories(string s);

        /// <summary>
        /// contains numerics and other characters without letters
        /// </summary>
        /// <param name="s"></param>
        /// <returns></returns>
        protected bool isWithoutLetters(string s)
        {
            bool result = s.Length > 0;
            for (int i = 0; i < s.Length; i++)
            {
                char ch = s[i];
                if (char.IsLetter(ch))
                {
                    result = false;
                    break;
                }
            }

            return result;
        }

        protected bool isNumeric(string s)
        {
            bool result = s.Length > 0;
            for (int i = 0; i < s.Length; i++)
            {
                char ch = s[i];
                if (!char.IsDigit(ch))
                {
                    result = false;
                    break;
                }
            }

            return result;
        }

        /// <summary>
        /// true when string contains both letters and digits
        /// </summary>
        /// <param name="s"></param>
        /// <returns></returns>
        protected bool isAlphaNumeric(string s)
        {
            bool result = false;
            bool isNumeric = false;
            bool isAlpha = false;
            for (int i = 0; i < s.Length; i++)
            {
                char ch = s[i];
                if (char.IsDigit(ch))
                {
                    isNumeric = true;
                }
                else if (char.IsLetter(ch))
                {
                    isAlpha = true;
                }
                else
                {
                    isNumeric = false; // sufficient to make the result false
                    break;
                }
            }

            result = isNumeric && isAlpha;
            return result;
        }

        protected bool isLetter(string s)
        {
            bool result = true;
            for (int i = 0; i < s.Length; i++)
            {
                char ch = s[i];
                if (ch == '\'')
                {
                    continue;
                }
                else if (!char.IsLetter(ch))
                {
                    result = false;
                    break;
                }
            }

            return result;
        }

        public abstract bool isSeparatorChar(char ch);

        private void addWord(StringBuilder sb, InputWordsList inputWordsList, int lineNumber)
        {
            if (sb.Length > 0)
            {
                inputWordsList.Add(sb.ToString(), lineNumber);
                sb.Length = 0;
            }
        }

        public virtual void Parse(string inputString, int maxWordsToParse)
        {
            clearAndReset();
            int offSetStart = 0;
            splitAndCategorizeWords(inputString, ref offSetStart, 0, maxWordsToParse);
            recognizePatterns();
        }

        public virtual void Parse(List<string> inputStrings, int maxWordsToParse)
        {
            clearAndReset();
            int offSetStart = 0;
            for (int lineNumber = 0; lineNumber < inputStrings.Count; lineNumber++)
            {
                splitAndCategorizeWords(inputStrings[lineNumber], ref offSetStart, lineNumber, maxWordsToParse);
            }

            recognizePatterns();
        }

        private void clearAndReset()
        {
            alreadyCheckedPatterns.Clear();
            RecognizedPatterns.Clear();
            patternsGroupList.Reset();
            inputWords.Clear();
            GroupOfWordsList.Clear();
        }

        private void splitAndCategorizeWords(string inputString, ref int offSetStart, int lineNumber,
            int maxWordsToParse)
        {
            InputWordsList tmpInputWordsList = splitInput(inputString, lineNumber);
            maxWordsToParse = Math.Min(maxWordsToParse, tmpInputWordsList.Count);
            for (int i = 0; i < maxWordsToParse; i++)
            {
                inputWords.Add(tmpInputWordsList[i]);
            }

            categorizeWords(tmpInputWordsList, ref offSetStart);
        }

        private InputWordsList splitInput(string inputString, int lineNumber)
        {
            InputWordsList result = new InputWordsList();
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < inputString.Length; i++)
            {
                char ch = inputString[i];
                if (isSeparatorChar(ch))
                {
                    addWord(sb, result, lineNumber);
                    sb.Append(ch);
                    addWord(sb, result, lineNumber);
                }
                else if (char.IsWhiteSpace(ch))
                {
                    addWord(sb, result, lineNumber);
                }
                else
                {
                    sb.Append(ch);
                }
            }

            addWord(sb, result, lineNumber);
            return result;
        }

        private void categorizeWords(InputWordsList tmpInputWordsList, ref int offSetStart)
        {
            for (int startingIndex = 0; startingIndex < tmpInputWordsList.Count; startingIndex++)
            {
                for (int wordsCount = MaxWordsInCategory; wordsCount > 0; wordsCount--)
                {
                    if (startingIndex + wordsCount <= tmpInputWordsList.Count)
                    {
                        CategorizedGroupOfWords groupOfWords = new CategorizedGroupOfWords(GroupOfWordsList,
                            tmpInputWordsList, startingIndex, wordsCount, offSetStart);
                        string groupedWords = groupOfWords.ToString();
                        groupOfWords.FoundCategories = findCategories(groupedWords);
                        GroupOfWordsList.AddIfNeeded(groupOfWords);
                    }
                }
            }

            offSetStart += tmpInputWordsList.Count;
        }

        private void recognizePatterns()
        {
            patternsGroupList.Reset();
            for (int i = 0; i < patternsGroupList.Count; i++)
            {
                PatternsGroup patternsGroup = patternsGroupList[i];
                recognizeSinglePatternsGroup(patternsGroup);
            }

            ChoosenPatternList.AssignLineNumbers();
        }

        private void recognizeSinglePatternsGroup(PatternsGroup patternsGroup)
        {
            for (int i = 0; i < patternsGroup.Count; i++)
            {
                Pattern pattern = patternsGroup[i];
                recognizeSinglePattern(patternsGroup, pattern, 0, 0, new RecognizedPatterns());
            }
        }

        private bool recognizeSinglePattern(PatternsGroup patternsGroup, Pattern pattern, int startIndex,
            int patternIndex, RecognizedPatterns candidatesToRecognizedPatterns)
        {
            bool result = false;
            if (startIndex <= inputWords.Count - 1)
            {
                if (startIndex < GroupOfWordsList.GroupOfWordsListSeparatedByStartIndex.Count)
                {
                    int remainedPatterns = pattern.Count - patternIndex - 1; // zero based...
                    List<CategorizedGroupOfWords> categorizedGroupOfWordsWithSameStartIndex =
                        GroupOfWordsList.GroupOfWordsListSeparatedByStartIndex[startIndex];
                    for (int i = 0; i < categorizedGroupOfWordsWithSameStartIndex.Count; i++)
                    {
                        CategorizedGroupOfWords categorizedGroupOfWords = categorizedGroupOfWordsWithSameStartIndex[i];
                        bool startFromBegining = false;
                        int newStartIndex = startIndex + categorizedGroupOfWords.Count;
                        if (categorizedGroupOfWords.FoundCategories.Count > 0)
                        {
                            if (categorizedGroupOfWords.FoundCategories.ContainsKey(pattern[patternIndex]))
                            {
                                if (patternIndex == pattern.Count - 1) // totally match
                                {
                                    pattern.Matched = true;
                                    result = true;
                                    RecognizedPattern recognizedPattern;
                                    if (pattern.Count > 1)
                                    {
                                        recognizedPattern = candidatesToRecognizedPatterns[patternsGroup.PatternId];
                                        RecognizedPatterns.AddIfNeeded(recognizedPattern);
                                    }
                                    else
                                    {
                                        recognizedPattern = RecognizedPatterns.AddIfNeeded(patternsGroup);
                                    }

                                    recognizedPattern.AdWordsIfNeeded(categorizedGroupOfWords);
                                    recognizedPattern.LockedActiveGroup = true;
                                    determineIsAlreadyChecked(pattern, ref startFromBegining, newStartIndex);
                                }
                                else
                                {
                                    if (startIndex + categorizedGroupOfWords.Count + remainedPatterns <=
                                        inputWords.Count)
                                    {
                                        // still matches, check forward
                                        RecognizedPattern recognizedPattern =
                                            candidatesToRecognizedPatterns.AddIfNeeded(patternsGroup);
                                        recognizedPattern.AdWordsIfNeeded(categorizedGroupOfWords);
                                        startFromBegining = !recognizeSinglePattern(patternsGroup, pattern,
                                            newStartIndex, patternIndex + 1, candidatesToRecognizedPatterns);
                                    }
                                }
                            }
                            else
                            {
                                determineIsAlreadyChecked(pattern, ref startFromBegining, newStartIndex);
                            }
                        }
                        else
                        {
                            determineIsAlreadyChecked(pattern, ref startFromBegining, newStartIndex);
                        }

                        if (startFromBegining)
                        {
                            recognizeSinglePattern(patternsGroup, pattern, newStartIndex, 0, new RecognizedPatterns());
                        }
                    }
                }
            }

            return result;
        }

        /// <summary>
        /// if already checked we don't run the pattern recognition again...
        /// </summary>
        /// <param name="pattern"></param>
        /// <param name="startFromBegining"></param>
        /// <param name="newStartIndex"></param>
        private void determineIsAlreadyChecked(Pattern pattern, ref bool startFromBegining, int newStartIndex)
        {
            Dictionary<int, bool> checkedStartIndexes;
            lock (alreadyCheckedPatterns)
            {
                alreadyCheckedPatterns.TryGetValue(pattern, out checkedStartIndexes);
                if (checkedStartIndexes != null)
                {
                    bool dummy;
                    if (!checkedStartIndexes.TryGetValue(newStartIndex, out dummy))
                    {
                        startFromBegining = true;
                    }
                }
                else
                {
                    checkedStartIndexes = new Dictionary<int, bool>();
                    alreadyCheckedPatterns.Add(pattern, checkedStartIndexes);
                    startFromBegining = true;
                }

                if (startFromBegining)
                {
                    checkedStartIndexes.Add(newStartIndex, false);
                }
            }
        }

        internal ChoosenPattern getMatchedValue(PatternsGroup patternsGroup, int minPosition, int maxPosition,
            PreferedOccurrence preferedOccurrence, PreferedWidth preferedWidth)
        {
            ChoosenPattern result = null;
            string text = "";
            if (patternsGroup.Matched)
            {
                CategorizedGroupOfWordsList resultCategorizedGroupOfWordsList = null;
                RecognizedPattern recognizedPattern = RecognizedPatterns[patternsGroup.PatternId];
                int minStart = int.MaxValue;
                int maxEnd = int.MinValue;
                if (recognizedPattern != null)
                {
                    // first we find the prefered position
                    for (int i = 0; i < recognizedPattern.Count; i++)
                    {
                        CategorizedGroupOfWordsList categorizedGroupOfWordsList = recognizedPattern[i];
                        if (!inputWords.IsRangeUsed(categorizedGroupOfWordsList.StartIndex,
                            categorizedGroupOfWordsList.LastIndex))
                        {
                            if (categorizedGroupOfWordsList.StartIndex < minPosition)
                            {
                                continue;
                            }

                            if (categorizedGroupOfWordsList.StartIndex > maxPosition)
                            {
                                continue;
                            }

                            if (preferedOccurrence == PreferedOccurrence.First)
                            {
                                if (categorizedGroupOfWordsList.StartIndex < minStart)
                                {
                                    minStart = categorizedGroupOfWordsList.StartIndex;
                                }
                            }
                            else if (preferedOccurrence == PreferedOccurrence.Last)
                            {
                                if (categorizedGroupOfWordsList.LastIndex > maxEnd)
                                {
                                    maxEnd = categorizedGroupOfWordsList.LastIndex;
                                }
                            }
                        }
                    }

                    // then fill the list of candidates with all groups of words from that prefered position
                    List<CategorizedGroupOfWordsList> candidatesGroups = new List<CategorizedGroupOfWordsList>();
                    for (int i = 0; i < recognizedPattern.Count; i++)
                    {
                        CategorizedGroupOfWordsList categorizedGroupOfWordsList = recognizedPattern[i];
                        if (preferedOccurrence == PreferedOccurrence.First)
                        {
                            if (categorizedGroupOfWordsList.StartIndex == minStart)
                            {
                                if (!inputWords.IsRangeUsed(categorizedGroupOfWordsList.StartIndex,
                                    categorizedGroupOfWordsList.LastIndex))
                                {
                                    candidatesGroups.Add(categorizedGroupOfWordsList);
                                }
                            }
                        }
                        else if (preferedOccurrence == PreferedOccurrence.Last)
                        {
                            if (categorizedGroupOfWordsList.LastIndex == maxEnd)
                            {
                                if (!inputWords.IsRangeUsed(categorizedGroupOfWordsList.StartIndex,
                                    categorizedGroupOfWordsList.LastIndex))
                                {
                                    candidatesGroups.Add(categorizedGroupOfWordsList);
                                }
                            }
                        }
                    }

                    // find the prefered width among remained candidates
                    int minChars = int.MaxValue;
                    int maxChars = int.MinValue;
                    string value;
                    int len;
                    for (int i = 0; i < candidatesGroups.Count; i++)
                    {
                        CategorizedGroupOfWordsList categorizedGroupOfWordsList = candidatesGroups[i];
                        value = categorizedGroupOfWordsList.ToString();
                        len = value.Length;
                        if (preferedWidth == PreferedWidth.Min)
                        {
                            if (len < minChars)
                            {
                                minChars = len;
                                resultCategorizedGroupOfWordsList = categorizedGroupOfWordsList;
                                text = value;
                            }
                        }
                        else if (preferedWidth == PreferedWidth.Max)
                        {
                            if (len > maxChars)
                            {
                                maxChars = len;
                                resultCategorizedGroupOfWordsList = categorizedGroupOfWordsList;
                                text = value;
                            }
                        }
                    }
                }

                if (resultCategorizedGroupOfWordsList != null)
                {
                    result = new ChoosenPattern(patternsGroup, text, resultCategorizedGroupOfWordsList.StartIndex,
                        resultCategorizedGroupOfWordsList.LastIndex);
                    addToChoosenPatternList(result);
                }
            }

            return result;
        }

        internal void addToChoosenPatternList(ChoosenPattern choosenPattern)
        {
            ChoosenPatternList.Add(choosenPattern);
            for (int i = choosenPattern.StartIndex; i <= choosenPattern.LastIndex; i++)
            {
                inputWords[i].Used = true;
            }
        }

        internal void removeFromChoosenPatternList(ref ChoosenPattern choosenPattern, bool permanently,
            ref int removedCount)
        {
            for (int i = choosenPattern.StartIndex; i <= choosenPattern.LastIndex; i++)
            {
                inputWords[i].Used = false;
            }

            if (permanently)
            {
                ChoosenPatternList.RemovePermanently(ref choosenPattern, ref removedCount);
            }
            else
            {
                ChoosenPatternList.RemoveTemp(ref choosenPattern);
            }
        }

        #endregion
    }

    internal class InputWordsList
    {
        #region Fields

        private List<InputWord> internalWordsList = new List<InputWord>();

        #endregion

        #region Indexers

        public InputWord this[Int32 index]
        {
            get { return internalWordsList[index]; }
        }

        #endregion

        #region Properties and Fields

        public Int32 Count
        {
            get { return internalWordsList.Count; }
        }

        #endregion

        #region Methods

        public Boolean IsRangeUsed(Int32 startIndex, Int32 lastIndex)
        {
            Boolean result = false;
            for (Int32 i = startIndex; i <= lastIndex; i++)
            {
                if (internalWordsList[i].Used)
                {
                    result = true;
                    break;
                }
            }

            return result;
        }

        public void Add(String word, Int32 lineNumber)
        {
            internalWordsList.Add(new InputWord(word, lineNumber));
        }

        public void Add(InputWord inputWord)
        {
            internalWordsList.Add(inputWord);
        }

        public void AddRange(InputWordsList inputWordsList)
        {
            for (Int32 i = 0; i < inputWordsList.Count; i++)
            {
                internalWordsList.Add(inputWordsList[i]);
            }
        }

        public void Clear()
        {
            internalWordsList.Clear();
        }

        #endregion
    }

    internal class InputWord
    {
        #region Fields

        private readonly String word;
        public readonly Int32 LineNumber;

        #endregion

        #region Constructors

        public InputWord(String word, Int32 lineNumber)
        {
            this.word = word;
            LineNumber = lineNumber;
        }

        #endregion

        #region Properties and Fields

        private Boolean used;

        /// <summary>
        /// to avoid using one word in more than one resulting pattern, e.g. "New York" is city and state and if we decide to be a city then it is marked Used and cannot be used again for state...
        /// </summary>
        public Boolean Used
        {
            get { return used; }
            set { used = value; }
        }

        #endregion

        #region Methods

        public override String ToString()
        {
            return word;
        }

        #endregion
    }
}
