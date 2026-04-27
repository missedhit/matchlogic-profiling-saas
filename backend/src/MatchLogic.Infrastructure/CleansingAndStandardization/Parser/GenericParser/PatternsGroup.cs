using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.CleansingAndStandardization.Parser.GenericParser
{
    /// <summary>
    /// list of PatternsGroup
    /// </summary>
    internal class PatternsGroupList
    {
        #region Fields

        private readonly List<PatternsGroup> internalPatternsGroupList = new List<PatternsGroup>();

        #endregion

        #region Indexers

        public PatternsGroup this[Int32 index]
        {
            get { return internalPatternsGroupList[index]; }
        }

        #endregion

        #region Methods

        public void Reset()
        {
            for (Int32 i = 0; i < internalPatternsGroupList.Count; i++)
            {
                PatternsGroup patternsGroup = internalPatternsGroupList[i];
                patternsGroup.Reset();
            }
        }

        public void Add(PatternsGroup patternsGroup)
        {
            internalPatternsGroupList.Add(patternsGroup);
        }

        #endregion

        #region Properties and Fields

        public Int32 Count
        {
            get { return internalPatternsGroupList.Count; }
        }

        #endregion
    }

    /// <summary>
    /// to identify full name we can for example use 3 patterns:
    /// Firstname - LastName, FirstName - UnrecognizedString and UnrecognizedString - FirstName
    /// every pattern has its own probability, obviously we can relay more on the first than other two patterns
    /// (the probability not implemented)
    /// </summary>
    internal class PatternsGroup
    {
        #region Fields

        private readonly List<Pattern> internalPaternsList = new List<Pattern>();

#if DEBUG
        public readonly String Name;
#endif

        #endregion

        #region Constructors

        public PatternsGroup(Int32 patternId
#if DEBUG
            , String name
#endif
        )
        {
            PatternId = patternId;
#if DEBUG
            Name = name;
#endif
        }

        #endregion

        #region Indexers

        public Pattern this[Int32 index]
        {
            get { return internalPaternsList[index]; }
        }

        #endregion

        #region Properties and Fields

        private Int32 patternId;

        public Int32 PatternId
        {
            get { return patternId; }
            set { patternId = value; }
        }

        public Int32 Count
        {
            get { return internalPaternsList.Count; }
        }

        public Boolean Matched
        {
            get { return matchedPattern != null; }
        }

        private Pattern matchedPattern = null;

        public Pattern MatchedPattern
        {
            get { return matchedPattern; }
            set { matchedPattern = value; }
        }

        private String matchedValue;

        public String MatchedValue
        {
            get { return matchedValue; }
            set { matchedValue = value; }
        }

        #endregion

        #region Methods

        public void Reset()
        {
            MatchedPattern = null;
            for (Int32 i = 0; i < internalPaternsList.Count; i++)
            {
                Pattern pattern = internalPaternsList[i];
                pattern.Reset();
            }
        }

        public void Add(Pattern pattern)
        {
            internalPaternsList.Add(pattern);
        }

        #endregion
    }

    /// <summary>
    ///
    /// </summary>
    internal class Pattern
    {
        #region Fields

        private List<Int32> patternToMatch;
        private readonly PatternsGroup patternsGroup;
#if DEBUG
        private readonly String[] categoryNames; // for easier debugging only...
#endif

        #endregion

        #region Indexers

        /// <summary>
        /// represents a categoryId from the specific pattern index
        /// e.g. the first category in pattern is FirstName
        /// and the seconf is LastName...
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        public Int32 this[Int32 index]
        {
            get { return patternToMatch[index]; }
        }

        #endregion

        #region Constructors

        public Pattern(List<Int32> patternToMatch, PatternsGroup patternsGroup, Type enumType)
        {
            this.patternToMatch = patternToMatch;
            this.patternsGroup = patternsGroup;
#if DEBUG
            categoryNames = new String[patternToMatch.Count];
            for (Int32 i = 0; i < patternToMatch.Count; i++)
            {
                categoryNames[i] = Enum.GetName(enumType, patternToMatch[i]);
            }
#endif
        }

        #endregion

        #region Properties and Fields

        public Int32 Count
        {
            get { return patternToMatch.Count; }
        }

        private Boolean matched = false;

        public Boolean Matched
        {
            get { return matched; }
            set
            {
                matched = value;
                if (Matched)
                {
                    patternsGroup.MatchedPattern = this;
                }
            }
        }

        #endregion

        #region Methods

        public void Reset()
        {
            matched = false;
        }

        #endregion
    }
}
