using MatchLogic.Application.Interfaces.DataMatching;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection.PortableExecutable;
using System.Text;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.FellegiSunter;

public class ParallelTermFrequencyIndex
{
    private Dictionary<string, Dictionary<string, int>> _fieldTermCounts = new();
    private Dictionary<string, int> _totalFieldCounts = new();

    public async Task IndexTerms(ChannelReader<IDictionary<string, object>> channelReader)
    {
        await foreach (var record in channelReader.ReadAllAsync())
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

public class ParallelEM //: IExpectationMaximisation
{
    private List<ProbabilisticMatchCriteria> _fields;
    private Dictionary<string, AgreementPattern> _patterns;
    private ParallelTermFrequencyIndex _termIndex;
    private double _priorMatchProbability;
    private readonly double _convergenceThreshold = 1e-6;
    private readonly int _maxIterations = 100;
    private List<double> _logLikelihoodHistory;
    private BayesianWeightCalculator _weightCalculator;
    private ProbabilisticOption _option;
    private int _decimalPlaces = 6;
    private readonly int _bufferSize = 1000;
    private readonly double _minProbability = 1e-10; // Prevent division by zero
    public ParallelEM(IOptions<ProbabilisticOption> option)
    {
        _patterns = new Dictionary<string, AgreementPattern>();
        _termIndex = new ParallelTermFrequencyIndex();
        _logLikelihoodHistory = new List<double>();
        _priorMatchProbability = 0.1;

        _option = option.Value;
        _decimalPlaces = _option.DecimalPlaces;
    }

    public void Initialize(List<ProbabilisticMatchCriteria> fields)       
    {
        _fields = fields;
        
        foreach (var field in _fields)
        {
            InitializeFieldLevels(field);
        }
    }
     private void InitializeFieldLevels(ProbabilisticMatchCriteria field)
    {
        foreach (var level in field.Settings.Levels)
        {
            switch (level.Name.ToLower())
            {
                case "exact":
                    level.M_Probability = 0.95;
                    level.U_Probability = 0.05;
                    break;
                case "high":
                    level.M_Probability = 0.85;
                    level.U_Probability = 0.15;
                    break;
                case "medium":
                    level.M_Probability = 0.50;
                    level.U_Probability = 0.50;
                    break;
                case "low":
                default:
                    level.M_Probability = 0.15;
                    level.U_Probability = 0.85;
                    break;
            }
            level.UpdateWeight();
        }
    }
    private List<double> DefaultMValues(int numLevels)
    {
        double proportionExactMatch = 0.95;
        double remainder = 1 - proportionExactMatch;
        double splitRemainder = remainder / (numLevels - 1);

        var mValues = new List<double>();
        // Add split values for all levels except exact match
        for (int i = 0; i < numLevels - 1; i++)
        {
            mValues.Add(splitRemainder);
        }
        // Add exact match probability
        mValues.Insert(0, proportionExactMatch);
        return mValues;
    }

    private List<double> Interpolate(double start, double end, int count)
    {
        var result = new List<double>();
        double step = (end - start) / (count - 1);
        for (int i = 0; i < count; i++)
        {
            result.Add(start + (step * i));
        }
        return result;
    }

    private double MatchWeightToBayesFactor(double weight)
    {
        return Math.Pow(2, weight); // Using 2^weight instead of e^weight
    }

    private List<double> DefaultUValues(int numLevels)
    {
        var mVals = DefaultMValues(numLevels);
        List<double> matchWeights;

        if (numLevels == 2)
        {
            matchWeights = new List<double> { -5 };
        }
        else
        {
            matchWeights = Interpolate(-5, 3, numLevels - 1);
        }
        matchWeights.Add(10);
        matchWeights.Reverse();
        var uVals = new List<double>();
        for (int i = 0; i < mVals.Count; i++)
        {
            double bayesFactor = MatchWeightToBayesFactor(matchWeights[i]);
            double u = mVals[i] / bayesFactor;
            uVals.Add(u);
        }
        return uVals;
    }
    //Previous
    //private void InitializeFieldLevels(ComparisonField field)
    //{
    //    foreach (var level in field.Settings.Levels)
    //    {
    //        if (level.Name == "Exact")
    //        {
    //            // For exact matches, use higher probabilities
    //            level.MProbability = 0.9;  // Start with high M probability for exact matches
    //            level.UProbability = 0.1;  // Low U probability for exact matches
    //        }
    //        else
    //        {
    //            // For other levels, use frequency-based initialization
    //            level.MProbability = Math.Max(0.1, 1.0 - field.Statistics.RandomAgreementProbability);
    //            level.UProbability = Math.Max(0.1, field.Statistics.RandomAgreementProbability);
    //        }
    //        level.UpdateWeight();

    //        //// Initialize M probabilities based on term frequencies
    //        //level.MProbability = Math.Max(0.1, 1.0 - field.Statistics.RandomAgreementProbability);
    //        //// Initialize U probabilities based on random agreement
    //        //level.UProbability = Math.Max(0.5, field.Statistics.RandomAgreementProbability); ;
    //        //level.UpdateWeight();
    //    }
    //}

    public async Task GeneratePatterns(ChannelReader<IDictionary<string, object>> records)
    {
        _patterns.Clear();

        var buffer = new List<IDictionary<string, object>>();

        await foreach (var record in records.ReadAllAsync())
        {
            // Compare incoming record with all buffered records
            foreach (var bufferedRecord in buffer)
            {
                CompareAndStorePattern(bufferedRecord, record);
            }

            buffer.Add(record);

            // When buffer reaches limit, remove oldest records
            if (buffer.Count > _bufferSize)
            {
                buffer.RemoveRange(0, _bufferSize / 2); // Remove half the buffer
            }
        }

        // Process remaining buffer pairs
        for (int i = 0; i < buffer.Count; i++)
        {
            for (int j = i + 1; j < buffer.Count; j++)
            {
                CompareAndStorePattern(buffer[i], buffer[j]);
            }
        }
    }
    private void CompareAndStorePattern(
        IDictionary<string, object> record1,
        IDictionary<string, object> record2)
    {
        var similarities = new Dictionary<string, double>();
        var levels = new Dictionary<string, ComparisonLevel>();

        foreach (var field in _fields)
        {
            var value1 = record1[field.FieldName]?.ToString() ?? "";
            var value2 = record2[field.FieldName]?.ToString() ?? "";

            double similarity = field.Comparator.Compare(value1, value2);
            similarity = Math.Round(similarity, _decimalPlaces);

            similarities[field.FieldName] = similarity;
            levels[field.FieldName] = field.Settings.GetLevel(similarity);
        }

        var pattern = new AgreementPattern(similarities, levels, _option);
        if (_patterns.TryGetValue(pattern.Key, out var existingPattern))
            existingPattern.Count++;
        else
            _patterns[pattern.Key] = pattern;
    }
    //Previous
    //private void InitializeFieldProbabilities(ComparisonField field)
    //{
    //    double discriminativePower = field.Statistics.Entropy;
    //    field.MProbability = 0.9 - (1 - discriminativePower) * 0.3;
    //    field.UProbability = 0.1 + (1 - discriminativePower) * 0.3;
    //    field.Weight = discriminativePower;
    //}

    public void RunEM()
    {
        int iterationsWithoutImprovement = 0;
        double previousLogLikelihood = double.MinValue;

        for (int iteration = 0; iteration < _maxIterations; iteration++)
        {
            // E-Step
            foreach (var pattern in _patterns.Values)
            {
                pattern.Posterior = CalculatePosterior(pattern);
            }

            // M-Step
            UpdateProbabilities();

            // Check convergence
            double logLikelihood = CalculateLogLikelihood();
            _logLikelihoodHistory.Add(logLikelihood);

            double improvement = Math.Abs(logLikelihood - previousLogLikelihood) /
                               (Math.Abs(previousLogLikelihood) + _minProbability);

            if (improvement < _convergenceThreshold)
            {
                iterationsWithoutImprovement++;
                if (iterationsWithoutImprovement >= 3)
                    break;
            }
            else
            {
                iterationsWithoutImprovement = 0;
            }

            previousLogLikelihood = logLikelihood;
        }
    }
    private double CalculatePosterior(AgreementPattern pattern)
    {
        double matchProb = Math.Log(_priorMatchProbability);
        double nonMatchProb = Math.Log(1 - _priorMatchProbability);

        foreach (var field in _fields)
        {
            var level = pattern.Levels[field.FieldName];
            matchProb += Math.Log(level.M_Probability);
            nonMatchProb += Math.Log(level.U_Probability);
        }

        // Convert back from log space with controlled range
        matchProb = Math.Exp(matchProb);
        nonMatchProb = Math.Exp(nonMatchProb);

        double denominator = matchProb + nonMatchProb;
        return denominator <= _minProbability ? 0.5 : (matchProb / denominator);
    }
    //Previous
    //private double CalculatePosterior(AgreementPattern pattern)
    //{
    //    // Calculate total weight using Bayes factors
    //    double totalWeight = 0;

    //    foreach (var field in _fields)
    //    {
    //        var similarity = pattern.Similarities[field.FieldName];
    //        var level = pattern.Levels[field.FieldName];
    //        double bayes_factor = level.BayesFactor;
    //        if (double.IsInfinity(bayes_factor))
    //        {
    //            bayes_factor = 1e300; // Approximate infinity
    //        }
    //        else if (bayes_factor == 0)
    //        {
    //            bayes_factor = 1e-300; // Approximate zero
    //        }
    //        totalWeight += level.Weight;
    //    }

    //    // Convert weight to probability using logistic function
    //    return 1.0 / (1.0 + Math.Exp(-totalWeight));
    //}

    private void UpdateProbabilities()
    {
        // Collect statistics for each comparison level        

        // Accumulate evidence for each level
        var levelStats = new Dictionary<string, Dictionary<double, (double matches, int total)>>();

        // Initialize statistics
        foreach (var field in _fields)
        {
            levelStats[field.FieldName] = field.Settings.Levels
                .ToDictionary(l => l.Threshold, _ => (matches: 0.0, total: 0));
        }

        // Accumulate statistics
        foreach (var pattern in _patterns.Values.Where(p => !double.IsNaN(p.Posterior)))
        {
            foreach (var field in _fields)
            {
                var similarity = pattern.Similarities[field.FieldName];
                var level = field.Settings.GetLevel(similarity);
                var stats = levelStats[field.FieldName][level.Threshold];

                // Apply term specificity adjustment
                double termWeight = 1.0;
                if (field.Statistics?.TermSpecificity > 0)
                {

                    termWeight = 1.0 + (field.Statistics.TermSpecificity * similarity);

                }

                levelStats[field.FieldName][level.Threshold] = (
                    stats.matches + (pattern.Posterior * pattern.Count * termWeight),
                    stats.total + pattern.Count
                );
            }
        }


        // Update probabilities for each field and level
        // Update probabilities
        foreach (var field in _fields)
        {
            foreach (var level in field.Settings.Levels)
            {
                if (levelStats[field.FieldName].TryGetValue(level.Threshold, out var stats)
                    && stats.total > 0)
                {
                    double mProb = stats.matches / stats.total;
                    double uProb = 1.0 - mProb;

                    // Apply minimum probability safeguard
                    mProb = Math.Max(mProb, _minProbability);
                    uProb = Math.Max(uProb, _minProbability);

                    // Normalize
                    double sum = mProb + uProb;
                    if (sum > 0)
                    {
                        level.M_Probability = Math.Round(mProb / sum, _decimalPlaces);
                        level.U_Probability = Math.Round(uProb / sum, _decimalPlaces);
                    }
                }
            }
        }

        // Update prior match probability
        UpdatePriorProbability();
    }

    private void UpdatePriorProbability()
    {
        double totalPairs = _patterns.Values.Sum(p => p.Count);
        if (totalPairs > 0)
        {
            double weightedMatches = _patterns.Values.Sum(p => p.Posterior * p.Count);
            _priorMatchProbability = Math.Round(
                Math.Max(weightedMatches / totalPairs, _minProbability),
                _decimalPlaces);
        }
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
            double weightSum = pattern.Levels.Values
                .Sum(level => Math.Log(Math.Max(
                    level.M_Probability / level.U_Probability,
                    _minProbability)));

            double probability = 1.0 / (1.0 + Math.Exp(-weightSum));
            return Math.Log(Math.Max(probability, _minProbability)) * pattern.Count;
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

public class ParallelFieldStatistics
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

    public static async Task<FieldStatistics> Calculate(
    IAsyncEnumerable<string> values,
    ParallelTermFrequencyIndex termIndex,
    string fieldName)
    {
        // First pass: Count totals and build frequencies
        var totalCount = 0;
        var nonNullCount = 0;
        var frequencyDict = new Dictionary<string, int>();

        await foreach (var value in values)
        {
            totalCount++;

            if (!string.IsNullOrEmpty(value))
            {
                nonNullCount++;
                if (!frequencyDict.ContainsKey(value))
                    frequencyDict[value] = 0;
                frequencyDict[value]++;
            }
        }

        // Convert raw counts to probabilities
        var frequencies = frequencyDict
            .ToDictionary(
                kvp => kvp.Key,
                kvp => (double)kvp.Value / nonNullCount);

        // Calculate statistics
        var entropy = -frequencies.Values.Sum(p => p * Math.Log(p));
        var maxEntropy = Math.Log(Math.Max(1, frequencies.Count));
        var completeness = (double)nonNullCount / totalCount;
        var distinctiveness = frequencies.Count / (double)nonNullCount;
        var qualityFactor = completeness * distinctiveness;

        // Calculate term specificity
        var termSpecificitySum = 0.0;
        var termCount = 0;
        foreach (var term in frequencies.Keys)
        {
            termSpecificitySum += termIndex.GetTermSpecificity(fieldName, term);
            termCount++;
        }
        var avgTermSpecificity = termCount > 0 ? termSpecificitySum / termCount : 0;

        var randomAgreement = CalculateRandomAgreementProbability(frequencies, qualityFactor);

        return new FieldStatistics
        {
            Entropy = entropy,
            MaxPossibleEntropy = maxEntropy,
            RelativeEntropy = maxEntropy > 0 ? entropy / maxEntropy : 1.0,
            ValueDistribution = frequencies,
            TermSpecificity = avgTermSpecificity,
            Completeness = completeness,
            Distinctiveness = distinctiveness,
            QualityFactor = qualityFactor,
            UniqueRatio = (double)frequencies.Count / nonNullCount,
            RandomAgreementProbability = randomAgreement
        };
    }
    private static double CalculateRandomAgreementProbability(
     Dictionary<string, double> frequencies, double qualityFactor)
    {
        var baseProb = frequencies.Values.Sum(f => f * f);
        return baseProb * qualityFactor; // Adjust for quality
    }
}
