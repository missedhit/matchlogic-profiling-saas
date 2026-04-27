using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.CleansingAndStandardization.Parser.GenericParser
{
    /// <summary>
    /// list of CategorizedGroupOfWords...
    /// </summary>
    internal class CategorizedGroupOfWordsList
    {
        #region Fields

        private readonly List<CategorizedGroupOfWords> internalGroupOfWordsList = new List<CategorizedGroupOfWords>();

        /// <summary>
        /// in element 0 are all groups which have aStrtingIndex == 0 and so on...
        /// </summary>
        internal readonly List<List<CategorizedGroupOfWords>> GroupOfWordsListSeparatedByStartIndex =
            new List<List<CategorizedGroupOfWords>>();

        ///
        internal readonly List<List<CategorizedGroupOfWords>> GroupOfWordsListSeparatedByLastIndex =
            new List<List<CategorizedGroupOfWords>>();

        #endregion

        #region Constructors

        public CategorizedGroupOfWordsList()
        {
        }

        #endregion

        #region Indexers

        public CategorizedGroupOfWords this[Int32 index]
        {
            get { return internalGroupOfWordsList[index]; }
        }

        #endregion

        #region Properties and Fields

        public Int32 Count
        {
            get { return internalGroupOfWordsList.Count; }
        }

        public Int32 LastIndex
        {
            get { return internalGroupOfWordsList[Count - 1].LastIndex; }
        }

        public Int32 StartIndex
        {
            get { return internalGroupOfWordsList[0].StartIndex; }
        }

        #endregion

        #region Methods

        public Boolean Contains(CategorizedGroupOfWords categorizedGroupOfWords)
        {
            return internalGroupOfWordsList.Contains(categorizedGroupOfWords);
        }

        public void AddIfNeeded(CategorizedGroupOfWords categorizedGroupOfWords)
        {
            if (!internalGroupOfWordsList.Contains(categorizedGroupOfWords))
            {
                internalGroupOfWordsList.Add(categorizedGroupOfWords);
                for (Int32 i = GroupOfWordsListSeparatedByStartIndex.Count;
                    i < categorizedGroupOfWords.StartIndex + 1;
                    i++)
                {
                    GroupOfWordsListSeparatedByStartIndex.Add(new List<CategorizedGroupOfWords>());
                }

                GroupOfWordsListSeparatedByStartIndex[GroupOfWordsListSeparatedByStartIndex.Count - 1]
                    .Add(categorizedGroupOfWords);

                for (Int32 i = GroupOfWordsListSeparatedByLastIndex.Count;
                    i < categorizedGroupOfWords.LastIndex + 1;
                    i++)
                {
                    GroupOfWordsListSeparatedByLastIndex.Add(new List<CategorizedGroupOfWords>());
                }

                GroupOfWordsListSeparatedByLastIndex[GroupOfWordsListSeparatedByLastIndex.Count - 1]
                    .Add(categorizedGroupOfWords);
            }
        }

        public void Clear()
        {
            internalGroupOfWordsList.Clear();
            GroupOfWordsListSeparatedByStartIndex.Clear();
            GroupOfWordsListSeparatedByLastIndex.Clear();
        }

        public override String ToString()
        {
            StringBuilder result = new StringBuilder();
            for (Int32 i = 0; i < internalGroupOfWordsList.Count; i++)
            {
                CategorizedGroupOfWords categorizedGroupOfWords = internalGroupOfWordsList[i];
                result.Append(categorizedGroupOfWords.ToString());
                if (i < internalGroupOfWordsList.Count - 1)
                {
                    result.Append(" ");
                }
            }

            return result.ToString();
        }

        #endregion
    }

    /// <summary>
    /// we try to find if all words together in unchanged order belong to some predefined category...
    /// </summary>
    internal class CategorizedGroupOfWords
    {
        #region Fields

        private List<String> words = new List<String>();

        /// <summary>
        /// the first word (or the only one) from the group has its index in the input string we are parsing
        /// e.g. input string is "abc def ghj klm" and group of words is "ghj klm", the StartIndex is the
        /// index of "ghj" word in the input string which is 2 in this case
        /// </summary>
        public readonly Int32 StartIndex;

        #endregion

        #region Constructors

        internal CategorizedGroupOfWords(CategorizedGroupOfWordsList groupOfWordsList, InputWordsList inputWords,
            Int32 startingIndex, Int32 wordsCount, Int32 offSetStart)
        {
            StartIndex = startingIndex + offSetStart;
            GroupOfWordsList = groupOfWordsList;
            for (Int32 i = 0; i < wordsCount; i++)
            {
                words.Add(inputWords[i + startingIndex].ToString());
            }
        }

        #endregion

        #region Properties and Fields

        private CategorizedGroupOfWordsList groupOfWordsList;

        public CategorizedGroupOfWordsList GroupOfWordsList
        {
            get { return groupOfWordsList; }
            set { groupOfWordsList = value; }
        }

        private Dictionary<Int32, Boolean> foundCategories;

        /// <summary>
        /// indexes of all categories where this group of words (or single word) belongs
        /// </summary>
        public Dictionary<Int32, Boolean> FoundCategories
        {
            get { return foundCategories; }
            set { foundCategories = value; }
        }

        /// <summary>
        /// the last word (or the only one) from the group has its index in the input string we are parsing
        /// e.g. input string is "abc def ghj klm" and group of words is "ghj klm", the LastIndex is the
        /// index of "klm" word in the input string which is 3 in this case
        /// </summary>
        public Int32 LastIndex
        {
            get { return StartIndex + words.Count - 1; }
        }

        public Int32 Count
        {
            get { return words.Count; }
        }

        #endregion

        #region Methods

        public override String ToString()
        {
            StringBuilder result = new StringBuilder();
            for (Int32 i = 0; i < words.Count; i++)
            {
                String inputWord = words[i];
                result.Append(inputWord);
                if (i < words.Count - 1)
                {
                    result.Append(' ');
                }
            }

            return result.ToString();
        }

        #endregion
    }
}
