using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.CleansingAndStandardization.Parser.GenericParser
{
    internal class ChoosenPatternList
    {
        #region Fields

        private List<ChoosenPattern> internalChoosenPatternList = new List<ChoosenPattern>();
        private readonly InputWordsList inputWordsList;
        private readonly Int32 undefindePatternId;

        #endregion

        #region Constructors

        internal ChoosenPatternList(InputWordsList inputWordsList, Int32 undefindePatternId)
        {
            this.inputWordsList = inputWordsList;
            this.undefindePatternId = undefindePatternId;
        }

        #endregion

        #region Indexers

        public ChoosenPattern this[Int32 index]
        {
            get { return internalChoosenPatternList[index]; }
        }

        #endregion

        #region Properties and Fields

        public Int32 Count
        {
            get { return internalChoosenPatternList.Count; }
        }

        #endregion

        #region Methods

        /// <summary>
        /// assigns the line number using the private member inputWordsList...
        /// </summary>
        public void AssignLineNumbers()
        {
            for (Int32 i = 0; i < internalChoosenPatternList.Count; i++)
            {
                ChoosenPattern choosenPattern = internalChoosenPatternList[i];
                choosenPattern.LineNumber = inputWordsList[choosenPattern.StartIndex].LineNumber;
            }
        }

        public void Add(ChoosenPattern choosenPattern)
        {
            internalChoosenPatternList.Add(choosenPattern);
            choosenPattern.LineNumber = inputWordsList[choosenPattern.StartIndex].LineNumber;
        }

        public Boolean Contains(ChoosenPattern choosenPattern)
        {
            return internalChoosenPatternList.Contains(choosenPattern);
        }

        public void RemoveTemp(ref ChoosenPattern choosenPattern)
        {
            internalChoosenPatternList.Remove(choosenPattern);
            choosenPattern = null;
        }

        public void RemovePermanently(ref ChoosenPattern choosenPattern, ref Int32 removedCount)
        {
            if (!choosenPattern.Removed)
            {
                internalChoosenPatternList.Remove(choosenPattern);
                choosenPattern.Removed = true;
                removedCount++;
            }

            //choosenPattern = null;
        }

        public void Clear()
        {
            internalChoosenPatternList.Clear();
        }

        public void SortAndGroupUnrecognizedWords(Int32 undefinedPatternId)
        {
            if (internalChoosenPatternList.Count > 0)
            {
                ChoosenPatternComparer choosenPatternComparer = new ChoosenPatternComparer();
                internalChoosenPatternList.Sort(choosenPatternComparer);
                determinePreviousNextInList();
                Int32 gapStartIndex = 0;
                Int32 gapLastIndex = 0;
                List<ChoosenPattern> unrecognizedChoosenPatternList = new List<ChoosenPattern>();

                for (Int32 i = 0; i < internalChoosenPatternList.Count; i++)
                {
                    ChoosenPattern choosenPattern = internalChoosenPatternList[i];
                    if (gapStartIndex < choosenPattern.StartIndex)
                    {
                        gapLastIndex = choosenPattern.StartIndex - 1;
                        addUndefinedPatternGroup(gapStartIndex, gapLastIndex, unrecognizedChoosenPatternList,
                            choosenPattern);
                    }

                    gapStartIndex = choosenPattern.LastIndex + 1;
                }

                ChoosenPattern lastChoosenPattern = internalChoosenPatternList[internalChoosenPatternList.Count - 1];
                if (lastChoosenPattern.LastIndex < inputWordsList.Count - 1)
                {
                    addUndefinedPatternGroup(gapStartIndex, inputWordsList.Count - 1, unrecognizedChoosenPatternList,
                        lastChoosenPattern);
                }

                for (Int32 i = 0; i < unrecognizedChoosenPatternList.Count; i++)
                {
                    internalChoosenPatternList.Add(unrecognizedChoosenPatternList[i]);
                }

                internalChoosenPatternList.Sort(choosenPatternComparer);
                AssignLineNumbers();
                determinePreviousNextInList();
                if (mergeUndefinedTypes(undefindePatternId))
                {
                    determinePreviousNextInList();
                }
            }
        }

        /// <summary>
        /// if two or more undefined patterns are near each other then we concatenate them
        /// </summary>
        private Boolean mergeUndefinedTypes(Int32 undefinedPatternId)
        {
            Boolean result = false;
            for (Int32 i = internalChoosenPatternList.Count - 1; i >= 0; i--)
            {
                ChoosenPattern choosenPattern = internalChoosenPatternList[i];
                if (choosenPattern.PatternsGroup.PatternId == undefindePatternId)
                {
                    if (choosenPattern.PreviousPatternId == undefindePatternId)
                    {
                        if (choosenPattern.IsPreviousInTheSameLine)
                        {
                            if (choosenPattern.StartIndex == choosenPattern.Previous.LastIndex + 1)
                            {
                                String text = choosenPattern.Previous.Text + " " + choosenPattern.Text;
                                ChoosenPattern concatenated = new ChoosenPattern(choosenPattern.PatternsGroup, text,
                                    choosenPattern.Previous.StartIndex, choosenPattern.LastIndex);
                                internalChoosenPatternList.RemoveAt(i);
                                internalChoosenPatternList[i - 1] = concatenated;
                                result = true;
                            }
                        }
                    }
                }
            }

            return result;
        }

        public Boolean RemoveIfBetween(ref ChoosenPattern choosenPattern, ChoosenPattern choosenPatternBefore,
            ChoosenPattern choosenPatternAfter, ref Int32 removedCount)
        {
            Boolean result = false;
            if ((choosenPattern != null) && (choosenPatternBefore != null) && (choosenPatternAfter != null))
            {
                if (choosenPattern.IsPatternIdBefore(choosenPatternBefore.PatternsGroup.PatternId) &&
                    choosenPattern.IsPatternIdAfter(choosenPatternAfter.PatternsGroup.PatternId))
                {
                    RemovePermanently(ref choosenPattern, ref removedCount);
                    result = true;
                }
            }

            return result;
        }

        /// <summary>
        /// true if the second parameter is before the first one (in the input string)
        /// </summary>
        /// <param name="choosenPattern"></param>
        /// <param name="choosenPatternBefore"></param>
        /// <param name="removedCount"></param>
        /// <returns></returns>
        public Boolean RemoveIfBefore(ref ChoosenPattern choosenPattern, ChoosenPattern choosenPatternBefore,
            ref Int32 removedCount)
        {
            Boolean result = false;
            if ((choosenPattern != null) && (choosenPatternBefore != null))
            {
                if (choosenPattern.IsPatternIdBefore(choosenPatternBefore.PatternsGroup.PatternId))
                {
                    RemovePermanently(ref choosenPattern, ref removedCount);
                    result = true;
                }
            }

            return result;
        }

        /// <summary>
        /// true if the second parameter is after the first one (in the input string)    
        /// </summary>
        /// <param name="choosenPattern"></param>
        /// <param name="choosenPatternAfter"></param>
        /// <param name="removedCount"></param>
        /// <returns></returns>
        public Boolean RemoveIfAfter(ref ChoosenPattern choosenPattern, ChoosenPattern choosenPatternAfter,
            ref Int32 removedCount)
        {
            Boolean result = false;
            if ((choosenPattern != null) && (choosenPatternAfter != null))
            {
                if (choosenPattern.IsPatternIdAfter(choosenPatternAfter.PatternsGroup.PatternId))
                {
                    RemovePermanently(ref choosenPattern, ref removedCount);
                    result = true;
                }
            }

            return result;
        }

        private void addUndefinedPatternGroup(Int32 gapStartIndex, Int32 gapLastIndex,
            List<ChoosenPattern> unrecognizedChoosenPatternList, ChoosenPattern choosenPattern)
        {
            Boolean allConcatenated;
            while (true)
            {
                Int32 memStartIndex = gapStartIndex;
                String text = concatenateInputWords(ref gapStartIndex, gapLastIndex, out allConcatenated);
                PatternsGroup undefinedPatternGroup = new PatternsGroup(undefindePatternId
#if DEBUG
                    , "undefined"
#endif
                );
                unrecognizedChoosenPatternList.Add(new ChoosenPattern(undefinedPatternGroup, text, memStartIndex,
                    gapLastIndex));
                if (allConcatenated)
                {
                    break;
                }
            }
        }

        private void determinePreviousNextInList()
        {
            for (Int32 i = 0; i < internalChoosenPatternList.Count; i++)
            {
                ChoosenPattern previous = (i > 0) ? internalChoosenPatternList[i - 1] : null;
                ChoosenPattern next = (i < internalChoosenPatternList.Count - 1)
                    ? internalChoosenPatternList[i + 1]
                    : null;
                ChoosenPattern current = internalChoosenPatternList[i];
                current.Previous = previous;
                current.Next = next;
            }
        }

        /// <summary>
        /// concatenates more 
        /// </summary>
        /// <param name="start"></param>
        /// <param name="end"></param>
        /// <param name="allConcatenated"></param>
        /// <returns></returns>
        private String concatenateInputWords(ref Int32 start, Int32 end, out Boolean allConcatenated)
        {
            allConcatenated = true;
            StringBuilder result = new StringBuilder();
            Int32 lineOfFirstWord = inputWordsList[start].LineNumber;
            for (Int32 i = start; i <= end; i++)
            {
                if (inputWordsList[i].LineNumber != lineOfFirstWord)
                {
                    start = i;
                    allConcatenated = false;
                    break;
                }

                result.Append(inputWordsList[i]);
                if (i != end)
                {
                    result.Append(" ");
                }
            }

            return result.ToString();
        }

        #endregion

        #region IComparer

        private class ChoosenPatternComparer : IComparer<ChoosenPattern>
        {
            #region IComparer<ChoosenPattern> Members

            public Int32 Compare(ChoosenPattern x, ChoosenPattern y)
            {
                Int32 result = 0;
                if (x.StartIndex > y.StartIndex)
                {
                    result = 1;
                }
                else if (x.StartIndex < y.StartIndex)
                {
                    result = -1;
                }

                return result;
            }

            #endregion
        }

        #endregion
    }

    /// <summary>
    /// in the first pass we choose max 1 pattern between recognized patterns (e.g. po box, state, etc..) and perform the higher level check (pass 2)...
    /// </summary>
    internal class ChoosenPattern
    {
        #region Fields

        public readonly PatternsGroup PatternsGroup;
        public readonly Int32 StartIndex;
        public readonly Int32 LastIndex;

        #endregion

        #region Constructors

        public ChoosenPattern(PatternsGroup patternsGroup, String text, Int32 startIndex, Int32 lastIndex)
        {
            PatternsGroup = patternsGroup;
            this.text = text;
            //     Offset = offset;
            StartIndex = startIndex;
            LastIndex = lastIndex;
        }

        #endregion

        #region Properties and Fields

        private String text;

        public String Text
        {
            get { return Removed ? null : text; }
        }

        private ChoosenPattern next = null;

        public ChoosenPattern Next
        {
            get { return next; }
            set { next = value; }
        }

        private ChoosenPattern previous = null;

        public ChoosenPattern Previous
        {
            get { return previous; }
            set { previous = value; }
        }

        public Boolean IsPreviousInTheSameLine
        {
            get
            {
                Boolean result = false;
                if (Previous != null)
                {
                    result = LineNumber == Previous.LineNumber;
                }

                return result;
            }
        }

        public Boolean IsNextInTheSameLine
        {
            get
            {
                Boolean result = false;
                if (Next != null)
                {
                    result = LineNumber == Next.LineNumber;
                }

                return result;
            }
        }

        public Boolean IsFirst
        {
            get { return (Previous == null); }
        }

        public Boolean IsLast
        {
            get { return (Next == null); }
        }

        /// <summary>
        /// true if patternId (the parameter) is in the previous pattern groups
        /// </summary>
        /// <param name="patternId">patternId to search for </param>
        /// <returns></returns>
        public Boolean IsPatternIdBefore(Int32 patternId)
        {
            Boolean result = false;
            if (Previous != null)
            {
                if (Previous.PatternsGroup.PatternId == patternId)
                {
                    result = true;
                }
                else
                {
                    return Previous.IsPatternIdBefore(patternId);
                }
            }

            return result;
        }

        /// <summary>
        /// true if patternId (the parameter) is in the next pattern groups
        /// </summary>
        /// <param name="patternId">patternId to search for </param>
        /// <returns></returns>
        public Boolean IsPatternIdAfter(Int32 patternId)
        {
            Boolean result = false;
            if (Next != null)
            {
                if (Next.PatternsGroup.PatternId == patternId)
                {
                    result = true;
                }
                else
                {
                    return Next.IsPatternIdAfter(patternId);
                }
            }

            return result;
        }

        const Int32 notExistingId = -1;

        public Int32 NextPatternId
        {
            get { return (Next != null) ? Next.PatternsGroup.PatternId : notExistingId; }
        }

        public Int32 PreviousPatternId
        {
            get { return (Previous != null) ? Previous.PatternsGroup.PatternId : notExistingId; }
        }

        private Int32 lineNumber = -1;

        public Int32 LineNumber
        {
            get { return lineNumber; }
            set { lineNumber = value; }
        }

        private Boolean removed;

        public Boolean Removed
        {
            get { return removed; }
            set { removed = value; }
        }

        #endregion
    }
}
