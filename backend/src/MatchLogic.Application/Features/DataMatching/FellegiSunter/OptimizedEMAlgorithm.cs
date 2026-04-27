using MatchLogic.Application.Features.DataMatching.Comparators;
using MatchLogic.Application.Interfaces.Comparator;
using MatchLogic.Application.Interfaces.DataMatching;
using MatchLogic.Domain.Entities;
using Microsoft.Extensions.Options;
using Microsoft.VisualBasic;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.AccessControl;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.FellegiSunter;

public class TermFrequencyIndex
{
    private Dictionary<string, Dictionary<string, int>> _fieldTermCounts = new();
    private Dictionary<string, int> _totalFieldCounts = new();

    public async void IndexTerms(List<IDictionary<string, object>> records)
    {
         foreach (var record in records)
        {
            foreach (var field in record)
            {
                if (!_fieldTermCounts.ContainsKey(field.Key))
                {
                    _fieldTermCounts[field.Key] = new Dictionary<string, int>();
                    _totalFieldCounts[field.Key] = 0;
                }

                var value = field.Value?.ToString() ?? "";
                if (!string.IsNullOrEmpty(value))
                {
                    if (!_fieldTermCounts[field.Key].ContainsKey(value))
                        _fieldTermCounts[field.Key][value] = 0;

                    _fieldTermCounts[field.Key][value]++;
                    _totalFieldCounts[field.Key]++;
                }
            }
        }
    }

    public double GetTermFrequency(string fieldName, string term)
    {
        if (_fieldTermCounts.TryGetValue(fieldName, out var termCounts) &&
            termCounts.TryGetValue(term, out var count))
        {
            return (double)count / _totalFieldCounts[fieldName];
        }
        return 0;
    }

    public double GetTermSpecificity(string fieldName, string term)
    {
        var frequency = GetTermFrequency(fieldName, term);
        return frequency > 0 ? -Math.Log(frequency) : 0;
    }
}
public class ComparisonLevel
{
    public string Name { get; set; }
    public double Threshold { get; set; }
    private double _mProbability;
    private double _uProbability;
    private const double MinProbability = 1e-10;

    public double M_Probability
    {
        get => _mProbability;
        set => _mProbability = Math.Max(value, MinProbability);
    }

    public double U_Probability
    {
        get => _uProbability;
        set => _uProbability = Math.Max(value, MinProbability);
    }

    public double Weight => Math.Log(M_Probability / U_Probability);

    public void UpdateWeight()
    {
        // Ensure probabilities are valid
        M_Probability = Math.Max(M_Probability, MinProbability);
        U_Probability = Math.Max(U_Probability, MinProbability);
    }

    public bool IsNullLevel { get; set; }
    public ComparisonLevel(string name, double threshold, ProbabilisticOption probabilisticOption, double initialMProb = 0.9, double initialUProb = 0.1)
    {
        //_decimalPlaces = probabilisticOption.DecimalPlaces;
        Name = name;
        Threshold = threshold;
        //MProbability = initialMProb;
        //UProbability = initialUProb;
        UpdateWeight();
    }

    //public void UpdateWeight()
    //{
    //    Weight = Math.Log(MProbability / UProbability);
    //}
}

public class BayesianWeightCalculator
{
    public double CalculateWeight(double mProb, double uProb)
    {
        // Weight is log of likelihood ratio (Bayes factor)
        return Math.Log(mProb / uProb);
    }

    public double CombineWeights(List<double> weights)
    {
        // Sum of log weights
        return weights.Sum();
    }

    public double WeightToProbability(double weight)
    {
        // Convert weight back to probability
        // P(M|comparison) = 1 / (1 + exp(-weight))
        return 1.0 / (1.0 + Math.Exp(-weight));
    }
}
public class FieldComparisonSettings
{
    public string FieldName { get; set; }
    public List<ComparisonLevel> Levels { get; }
    public bool HandleNullValues { get; set; }
    public double NullValueAgreementWeight { get; set; }

    // Example levels for a name field:
    // exact match (1.0)
    // high similarity (>0.9)
    // medium similarity (>0.8)
    // low similarity (>0.7)
    // no match (<0.7)

    private ProbabilisticOption _probabilisticOption;
    public FieldComparisonSettings(string fieldName, ProbabilisticOption probabilisticOption)
    {
        FieldName = fieldName;
        Levels = new List<ComparisonLevel>
        {
            new ComparisonLevel("Exact", 1.0,probabilisticOption, 0.9, 0.1),
            new ComparisonLevel("High", 0.8,probabilisticOption, 0.8, 0.2),
            new ComparisonLevel("Medium", 0.6,probabilisticOption, 0.6, 0.3),
            new ComparisonLevel("Low", 0.2, probabilisticOption, 0.4, 0.4)
        };
        HandleNullValues = true;
        NullValueAgreementWeight = 0.0;
        _probabilisticOption = probabilisticOption;
    }

    public ComparisonLevel GetLevel(double similarity)
    {
        return Levels.FirstOrDefault(l => similarity >= l.Threshold)
            ?? Levels.Last();  // Default to lowest level
    }
}
public class SparseVector
{
    private Dictionary<int, double> _values = new Dictionary<int, double>();

    public void Add(int index, double value)
    {
        if (Math.Abs(value) > 1e-10)
        {
            _values[index] = value;
        }
    }

    public double Get(int index) => _values.TryGetValue(index, out double value) ? value : 0.0;

    public IEnumerable<(int Index, double Value)> NonZeroEntries() =>
        _values.Select(kv => (kv.Key, kv.Value));
}

public class SimilarityMatrix
{
    private Dictionary<string, List<(int PairIndex, double Value)>> _matrix = new Dictionary<string, List<(int PairIndex, double Value)>>();
    private int _pairCount;

    public void AddSimilarity(string fieldName, int pairIndex, double value)
    {
        if (!_matrix.ContainsKey(fieldName))
        {
            _matrix[fieldName] = new List<(int, double)>();
        }
        if (Math.Abs(value) > 1e-10)
        {
            _matrix[fieldName].Add((pairIndex, value));
        }
    }

    public double GetSimilarity(string fieldName, int pairIndex) =>
        _matrix[fieldName].FirstOrDefault(x => x.PairIndex == pairIndex).Value;

    public IEnumerable<(int PairIndex, double Value)> GetFieldSimilarities(string fieldName) =>
        _matrix.TryGetValue(fieldName, out var similarities) ? similarities : Enumerable.Empty<(int, double)>();
}

public class AgreementPattern
{
    public Dictionary<string, double> Similarities { get; }
    public Dictionary<string, ComparisonLevel> Levels { get; }
    public int Count { get; set; }
    public double Weight { get; private set; }
    public double Posterior { get; set; }
    public string Key { get; }

    public int _decimalPlaces;
    public AgreementPattern(Dictionary<string, double> similarities,
                          Dictionary<string, ComparisonLevel> levels, ProbabilisticOption probabilisticOption)
    {
        _decimalPlaces = probabilisticOption.DecimalPlaces;
        Similarities = similarities;
        Levels = levels;
        Count = 1;
        Key = GenerateKey(similarities);
        CalculateWeight();
    }

    private void CalculateWeight()
    {
        Weight = Math.Round(Levels.Values.Sum(level => level.Weight), _decimalPlaces);
    }

    private string GenerateKey(Dictionary<string, double> similarities)
    {
        var format = $"F{_decimalPlaces}";
        return string.Join("|", similarities.OrderBy(x => x.Key)
            .Select(x => $"{x.Key}:{x.Value.ToString(format)}"));
    }
}

public class FieldStatistics
{
    public double Entropy { get; set; }
    public double MaxPossibleEntropy { get; set; }
    public double TermSpecificity { get; set; }
    public double RelativeEntropy { get; set; }
    public double UniqueRatio { get; set; }
    public double RandomAgreementProbability { get; set; }
    public Dictionary<string, double> ValueDistribution { get; set; }

    // New properties for better frequency analysis
    public double Completeness { get; set; }  // Measure of non-null values
    public double Distinctiveness { get; set; }  // Measure of value uniqueness
    public double QualityFactor { get; set; }  // Overall field quality score

    public static FieldStatistics Calculate(IEnumerable<string> values, TermFrequencyIndex termIndex, string fieldName)
    {
        var valueList = values.ToList();
        var totalCount = valueList.Count;
        var nonNullCount = valueList.Count(v => !string.IsNullOrEmpty(v));

        var frequencies = valueList
            .Where(v => !string.IsNullOrEmpty(v))
            .GroupBy(x => x)
            .ToDictionary(
                g => g.Key,
                g => (double)g.Count() / nonNullCount
            );

        var entropy = -frequencies.Values.Sum(p => p * Math.Log(p));
        var maxEntropy = Math.Log(Math.Max(1, frequencies.Count)); // Prevent log(0)
        var completeness = (double)nonNullCount / totalCount;
        var distinctiveness = frequencies.Count / (double)nonNullCount;
        var qualityFactor = completeness * distinctiveness;

        var randomAgreement = CalculateRandomAgreementProbability(frequencies, qualityFactor);

        // Now we have fieldName to use with termIndex
        var avgTermSpecificity = frequencies.Keys
            .Select(term => termIndex.GetTermSpecificity(fieldName, term))
            .DefaultIfEmpty(0)
            .Average();

        return new FieldStatistics
        {
            Entropy = entropy,
            MaxPossibleEntropy = maxEntropy,
            RelativeEntropy = maxEntropy > 0 ? entropy / maxEntropy : 1.0, // Handle division by zero
        ValueDistribution = frequencies,
            TermSpecificity = avgTermSpecificity,
            Completeness = completeness,
            Distinctiveness = distinctiveness,
            QualityFactor = qualityFactor,
            UniqueRatio = (double)frequencies.Count / nonNullCount,
            RandomAgreementProbability = randomAgreement  // Now assigned
        };
    }
    private static double CalculateRandomAgreementProbability(
       Dictionary<string, double> frequencies, double qualityFactor)
    {
        var baseProb = frequencies.Values.Sum(f => f * f);
        return baseProb * qualityFactor; // Adjust for quality
    }
}

public class ProbabilisticMatchCriteria: MatchCriteria
{
    private const double EPSILON = 1e-10;
    private const double SMOOTHING_FACTOR = 0.01;
    
    public IComparator Comparator { get; }
    public FieldComparisonSettings Settings { get; }
    public FieldStatistics Statistics { get; set; }
    //public double Weight { get; set; }

    private int _decimalPlaces;
    public ProbabilisticMatchCriteria(string fieldName, IComparatorBuilder comparatorBuilder,Dictionary<ArgsValue,string> args , ProbabilisticOption probabilisticOption)
    {
        _decimalPlaces = probabilisticOption.DecimalPlaces;
        FieldName = fieldName;
        Arguments = args;
        Comparator = comparatorBuilder.WithArgs(Arguments).Build();
        Settings = new FieldComparisonSettings(fieldName, probabilisticOption);
        Weight = 1.0;
    }

    public double CompareValues(string value1, string value2)
    {
        if (string.IsNullOrEmpty(value1) || string.IsNullOrEmpty(value2))
        {
            if (Settings.HandleNullValues &&
                string.IsNullOrEmpty(value1) && string.IsNullOrEmpty(value2))
                return Settings.NullValueAgreementWeight;
            return 0;
        }

        double similarity = Comparator.Compare(value1, value2);
        return Settings.GetLevel(similarity).Weight;
    }

    public void UpdateLevelProbabilities(Dictionary<double, (double matches, int total)> levelStats)
    {
        foreach (var level in Settings.Levels)
        {
            if (levelStats.TryGetValue(level.Threshold, out var stats))
            {
                if (stats.total > 0)
                {
                    double mProb = stats.matches / stats.total;
                    double uProb = 1.0 - mProb;

                    // Apply minimum probability safeguard
                    mProb = Math.Max(mProb, EPSILON);
                    uProb = Math.Max(uProb, EPSILON);

                    // Normalize to ensure m + u ≈ 1
                    double sum = mProb + uProb;
                    if (sum > 0)
                    {
                        mProb /= sum;
                        uProb /= sum;
                    }

                    //level.MProbability = Math.Round(mProb, _decimalPlaces);
                    //level.UProbability = Math.Round(uProb, _decimalPlaces);
                }
            }
        }
    }

    private double RoundProbability(double value, int decimalPlaces)
    {
        // First apply bounds
        value = Math.Max(EPSILON, Math.Min(1 - EPSILON, value));

        // Calculate the smallest non-zero value for given decimal places
        double minValue = Math.Pow(10, -decimalPlaces);
        double maxValue = 1 - minValue;

        // Then do rounding but ensure we don't get exactly 0 or 1
        double rounded = Math.Round(value, decimalPlaces);
        if (rounded >= 1)
            rounded = maxValue;
        else if (rounded <= 0)
            rounded = minValue;

        return rounded;
    }
}

public class OptimizedEM: IExpectationMaximisation
{
    private  List<ProbabilisticMatchCriteria> _fields;
    private Dictionary<string, AgreementPattern> _patterns;
    private TermFrequencyIndex _termIndex;
    private double _priorMatchProbability;
    private readonly double _convergenceThreshold = 1e-6;
    private readonly int _maxIterations = 100;
    private List<double> _logLikelihoodHistory;
    private BayesianWeightCalculator _weightCalculator;
    private ProbabilisticOption _option;
    private int _decimalPlaces = 6;
    public OptimizedEM(IOptions<ProbabilisticOption> option)
    {        
        _patterns = new Dictionary<string, AgreementPattern>();
        _termIndex = new TermFrequencyIndex();
        _logLikelihoodHistory = new List<double>();
        _priorMatchProbability = 0.1; //
        _option = option.Value;
        _decimalPlaces = _option.DecimalPlaces;
    }

    public void Initialize(List<ProbabilisticMatchCriteria> fields, IAsyncEnumerable<IDictionary<string, object>> records)
    {
        _fields = fields;
        // Index terms for frequency calculations
        var recordList =  records.ToListAsync().Result;
        _termIndex.IndexTerms(recordList);

        // Initialize field statistics and probabilities
        foreach (var field in _fields)
        {
            var values = records.Select(r => r[field.FieldName]?.ToString() ?? "").ToListAsync().Result;
            field.Statistics = FieldStatistics.Calculate(values, _termIndex, field.FieldName);
            InitializeFieldLevels(field);
        }

        // Generate comparison patterns
        GeneratePatterns(recordList);
    }
    private void InitializeFieldLevels(ProbabilisticMatchCriteria field)
    {
        foreach (var level in field.Settings.Levels)
        {
            switch (level.Name)
            {
                case "Exact":
                    level.M_Probability = 0.90;
                    level.U_Probability = 0.05;
                    break;
                case "High":
                    level.M_Probability = 0.70;
                    level.U_Probability = 0.10;
                    break;
                case "Medium":
                    level.M_Probability = 0.40;
                    level.U_Probability = 0.20;
                    break;
                case "Low":
                    level.M_Probability = 0.20;
                    level.U_Probability = 0.30;
                    break;
            }
            level.UpdateWeight();
        }
    }    

    private async void GeneratePatterns(List<IDictionary<string, object>> records)
    {
        _patterns.Clear();
       
        for (int i = 0; i < records.Count; i++)
        {
            for (int j = i + 1; j < records.Count; j++)
            {
                var similarities = new Dictionary<string, double>();
                var levels = new Dictionary<string, ComparisonLevel>();

                foreach (var field in _fields)
                {
                    var value1 = records[i][field.FieldName]?.ToString() ?? "";
                    var value2 = records[j][field.FieldName]?.ToString() ?? "";

                    double similarityWithoutRound = field.Comparator.Compare(value1, value2);
                    double similarity = Math.Round(similarityWithoutRound, _decimalPlaces);
                    similarities[field.FieldName] = similarity;

                    // Store the comparison level for weight calculation
                    levels[field.FieldName] = field.Settings.GetLevel(similarity);
                }

                var pattern = new AgreementPattern(similarities, levels, _option);
                if (_patterns.TryGetValue(pattern.Key, out var existingPattern))
                    existingPattern.Count++;
                else
                    _patterns[pattern.Key] = pattern;
            }
        }
    }

    public void RunEM()
    {
        int iterationsWithoutImprovement = 0;
        double previousLogLikelihood = double.MinValue;

        for (int iteration = 0; iteration < _maxIterations; iteration++)
        {
            // E-Step: Calculate posteriors using Bayes factors
            foreach (var pattern in _patterns.Values)
            {
                pattern.Posterior = CalculatePosterior(pattern);
            }

            // M-Step: Update probabilities and weights
            UpdateProbabilities();

            // Check convergence
            double logLikelihood = CalculateLogLikelihood();
            _logLikelihoodHistory.Add(logLikelihood);

            double improvement = (logLikelihood - previousLogLikelihood) /
                               Math.Abs(previousLogLikelihood + 1e-10);

            if (Math.Abs(improvement) < _convergenceThreshold)
                iterationsWithoutImprovement++;
            else
                iterationsWithoutImprovement = 0;

            if (iterationsWithoutImprovement >= 3)
                break;

            previousLogLikelihood = logLikelihood;
        }
    }
    private double CalculatePosterior(AgreementPattern pattern)
    {
        double matchProb = 1.0;
        double nonMatchProb = 1.0;

        foreach (var field in _fields)
        {
            var similarity = pattern.Similarities[field.FieldName];
            var level = pattern.Levels[field.FieldName];

            // Multiply probabilities instead of adding weights
            matchProb *= level.M_Probability;
            nonMatchProb *= level.U_Probability;
        }

        // Use Bayes theorem with prior
        double prior = _priorMatchProbability;
        return Math.Round((matchProb * prior) /
               ((matchProb * prior) + (nonMatchProb * (1 - prior))), _decimalPlaces);
    }   

    private void UpdateProbabilities()
    {
        // Collect statistics for each comparison level
        var levelStats = new Dictionary<string, Dictionary<double, (double matches, int total)>>();

        foreach (var field in _fields)
        {
            levelStats[field.FieldName] = new Dictionary<double, (double matches, int total)>();
            foreach (var level in field.Settings.Levels)
            {
                levelStats[field.FieldName][level.Threshold] = (0, 0);
            }
        }

        // Accumulate evidence for each level
        foreach (var pattern in _patterns.Values)
        {
            if (!double.IsNaN(pattern.Posterior))
            {
                foreach (var field in _fields)
                {
                    var similarity = pattern.Similarities[field.FieldName];
                    if (Math.Abs(similarity - 1.0) < 1e-10)  // Perfect match
                    {
                        var level = field.Settings.Levels.First(l => l.Name == "Exact");
                        var current = levelStats[field.FieldName][level.Threshold];
                        levelStats[field.FieldName][level.Threshold] = (
                        current.matches + pattern.Count,  // Accumulate matches
                        current.total + pattern.Count     // Accumulate total
                        );
                    }
                    else
                    {
                        var level = field.Settings.GetLevel(similarity);
                        var current = levelStats[field.FieldName][level.Threshold];
                        levelStats[field.FieldName][level.Threshold] = (
                               current.matches + (pattern.Posterior * pattern.Count),  // Accumulate weighted matches
                               current.total + pattern.Count                          // Accumulate total
                           );
                    }
                }
            }
        }

        // Update probabilities for each field and level
        foreach (var field in _fields)
        {
            field.UpdateLevelProbabilities(levelStats[field.FieldName]);
        }

        // Update prior match probability
        UpdatePriorProbability();
    }

    private void UpdatePriorProbability()
    {
        double weightedMatches = _patterns.Values.Sum(p => p.Posterior * p.Count);
        double totalPairs = _patterns.Values.Sum(p => p.Count);        
        _priorMatchProbability = Math.Round(weightedMatches / totalPairs, _decimalPlaces);
    }

    //private double CalculateLogLikelihood()
    //{
    //    double logLikelihood = 0;

    //    foreach (var pattern in _patterns.Values)
    //    {
    //        double matchProb = 1.0;
    //        double nonMatchProb = 1.0;

    //        foreach (var field in _fields)
    //        {
    //            var level = pattern.Levels[field.FieldName];
    //            matchProb *= level.M_Probability;
    //            nonMatchProb *= level.U_Probability;
    //        }

    //        // Add weighted contribution from both match and non-match cases
    //        logLikelihood += pattern.Count * (
    //            pattern.Posterior * Math.Log(matchProb) +
    //            (1 - pattern.Posterior) * Math.Log(nonMatchProb)
    //        );
    //    }

    //    return logLikelihood;
    //}
    //Previous
    private double CalculateLogLikelihood()
    {
        return _patterns.Values.Sum(pattern =>
        {
            double weight = 0;
            foreach (var level in pattern.Levels.Values)
            {
                weight += level.Weight;
            }
            return Math.Log(1.0 / (1.0 + Math.Exp(-weight))) * pattern.Count;
        });
    }

    public IEnumerable<(string FieldName, double MProb, double UProb)> GetResults()
    {
        return _fields.Select(f => (
            f.FieldName,
            f.Settings.Levels.Max(l => l.M_Probability),  // Use highest M probability
            f.Settings.Levels.Min(l => l.U_Probability)   // Use lowest U probability
        ));
    }

    public IEnumerable<(string Pattern, int Count, double Posterior)> GetPatterns()
    {
        return _patterns.Values
            .OrderByDescending(p => p.Posterior)
            .Select(p => (p.Key, p.Count, p.Posterior));
    }
}