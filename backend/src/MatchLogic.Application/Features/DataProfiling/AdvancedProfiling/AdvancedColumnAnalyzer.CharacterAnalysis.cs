using MatchLogic.Domain.DataProfiling;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataProfiling.AdvancedProfiling;

public partial class AdvancedColumnAnalyzer
{
    /// <summary>
    /// Standard character analysis
    /// </summary>
    private void AnalyzeCharacters(string value, IDictionary<string, object> fullRow, long rowNumber)
    {
        bool hasLetters = false;
        bool hasDigits = false;
        bool hasPunctuation = false;
        bool hasSpecialChars = false;
        bool hasNonPrintable = false;
        bool onlyLetters = true;
        bool onlyDigits = true;
        bool hasLeadingSpace = false;

        // Check for numeric string first (special case)
        bool isNumericString = double.TryParse(value, out _);
        if (isNumericString && !string.IsNullOrEmpty(value) && value.All(c => char.IsDigit(c) || c == '.' || c == '-' || c == '+'))
        {
            onlyDigits = true;
            Interlocked.Increment(ref _digitsOnlyCount);
            if (_characteristicRows[ProfileCharacteristic.NumbersOnly].Count < _options.MaxRowsPerCategory)
            {
                _characteristicRows[ProfileCharacteristic.NumbersOnly].Add(
                    CreateRowReference(fullRow, value, rowNumber));
            }
        }

        // Check for leading spaces
        if (value.Length > 0 && char.IsWhiteSpace(value[0]))
        {
            Interlocked.Increment(ref _leadingSpacesCount);
            hasLeadingSpace = true;

            // Store leading space row reference
            if (_characteristicRows[ProfileCharacteristic.WithLeadingSpaces].Count < _options.MaxRowsPerCategory)
            {
                _characteristicRows[ProfileCharacteristic.WithLeadingSpaces].Add(
                    CreateRowReference(fullRow, value, rowNumber));
            }
        }

        // Process each character
        foreach (char c in value)
        {
            if (char.IsLetter(c))
            {
                //Interlocked.Increment(ref _lettersCount);
                hasLetters = true;
                onlyDigits = false;
            }
            else if (char.IsDigit(c))
            {
                //Interlocked.Increment(ref _digitsCount);
                hasDigits = true;
                onlyLetters = false;
            }
            else if (char.IsWhiteSpace(c))
            {
                //Interlocked.Increment(ref _whitespaceCount);
                onlyLetters = false;
                onlyDigits = false;
            }
            else if (char.IsPunctuation(c))
            {
                //Interlocked.Increment(ref _punctuationCount);
                hasPunctuation = true;
                onlyLetters = false;
                onlyDigits = false;
            }
            else if (char.IsControl(c) || c == '\t' || c == '\n' || c == '\r' || c == '\0' || c == '\b' || c == '\a')
            {
                // Explicitly check for common control characters
                //Interlocked.Increment(ref _nonPrintableCount);
                hasNonPrintable = true;
                onlyLetters = false;
                onlyDigits = false;
            }
            else
            {
                //Interlocked.Increment(ref _specialCharCount);
                hasSpecialChars = true;
                onlyLetters = false;
                onlyDigits = false;
            }
        }
        // If value has any letter
        if (hasLetters)
        {
            Interlocked.Increment(ref _lettersCount);
            if (_characteristicRows[ProfileCharacteristic.Letters].Count < _options.MaxRowsPerCategory)
            {
                _characteristicRows[ProfileCharacteristic.Letters].Add(
                    CreateRowReference(fullRow, value, rowNumber));
            }
        }
        // If value has any digit
        if (hasDigits)
        {
            Interlocked.Increment(ref _digitsCount);
            if (_characteristicRows[ProfileCharacteristic.Numbers].Count < _options.MaxRowsPerCategory)
            {
                _characteristicRows[ProfileCharacteristic.Numbers].Add(
                    CreateRowReference(fullRow, value, rowNumber));
            }
        }
        // If value has any punctuation
        if (hasPunctuation)
        {
            Interlocked.Increment(ref _punctuationCount);
            if (_characteristicRows[ProfileCharacteristic.WithPunctuation].Count < _options.MaxRowsPerCategory)
            {
                _characteristicRows[ProfileCharacteristic.WithPunctuation].Add(
                    CreateRowReference(fullRow, value, rowNumber));
            }
        }
        // if value has any non printable characters
        if (hasNonPrintable)
        {
            Interlocked.Increment(ref _nonPrintableCount);
            if (_characteristicRows[ProfileCharacteristic.WithNonPrintable].Count < _options.MaxRowsPerCategory)
            {
                _characteristicRows[ProfileCharacteristic.WithNonPrintable].Add(
                    CreateRowReference(fullRow, value, rowNumber));
            }
        }
        // if value has any special characters
        if (hasSpecialChars)
        {
            Interlocked.Increment(ref _specialCharCount);
            if (_characteristicRows[ProfileCharacteristic.WithSpecialChars].Count < _options.MaxRowsPerCategory)
            {
                _characteristicRows[ProfileCharacteristic.WithSpecialChars].Add(
                    CreateRowReference(fullRow, value, rowNumber));
            }
        }
        UpdateCharacteristicRows(value, fullRow, rowNumber, onlyLetters, onlyDigits, hasLetters, hasDigits,
            hasPunctuation, hasSpecialChars, hasNonPrintable);
    }

    /// <summary>
    /// SIMD-optimized character analysis for string values
    /// </summary>
    private void AnalyzeCharactersWithSimd(string value, IDictionary<string, object> fullRow, long rowNumber)
    {
        // For very short strings, use the standard approach
        if (value.Length < 16)
        {
            AnalyzeCharacters(value, fullRow, rowNumber);
            return;
        }

        bool hasLetters = false;
        bool hasDigits = false;
        bool hasPunctuation = false;
        bool hasSpecialChars = false;
        bool hasNonPrintable = false;
        bool onlyLetters = true;
        bool onlyDigits = true;

        // Check for numeric string first (special case)
        bool isNumericString = double.TryParse(value, out _);
        if (isNumericString && !string.IsNullOrEmpty(value) && value.All(c => char.IsDigit(c) || c == '.' || c == '-' || c == '+'))
        {
            onlyDigits = true;
            Interlocked.Increment(ref _digitsOnlyCount);
            if (_characteristicRows[ProfileCharacteristic.NumbersOnly].Count < _options.MaxRowsPerCategory)
            {
                _characteristicRows[ProfileCharacteristic.NumbersOnly].Add(
                    CreateRowReference(fullRow, value, rowNumber));
            }
        }

        // Check for leading spaces
        if (value.Length > 0 && char.IsWhiteSpace(value[0]))
        {
            Interlocked.Increment(ref _leadingSpacesCount);

            if (_characteristicRows[ProfileCharacteristic.WithLeadingSpaces].Count < _options.MaxRowsPerCategory)
            {
                _characteristicRows[ProfileCharacteristic.WithLeadingSpaces].Add(
                    CreateRowReference(fullRow, value, rowNumber));
            }
        }

        // Fast SIMD-based character analysis
        if (Vector.IsHardwareAccelerated && value.Length >= Vector<ushort>.Count)
        {
            ReadOnlySpan<char> charSpan = value.AsSpan();
            int letterCount = 0;
            int digitCount = 0;
            int whitespaceCount = 0;
            int punctuationCount = 0;
            int controlCount = 0;
            int specialCount = 0;

            // Process in SIMD vector chunks
            int vectorized = charSpan.Length - (charSpan.Length % Vector<ushort>.Count);
            int i = 0;

            // SIMD optimization for character counting
            for (; i < vectorized; i += Vector<ushort>.Count)
            {
                var chunk = MemoryMarshal.Cast<char, ushort>(charSpan.Slice(i, Vector<ushort>.Count));
                var vec = new Vector<ushort>(chunk);

                // Digit check (0-9: 48-57)
                var digitLower = Vector.GreaterThanOrEqual(vec, new Vector<ushort>(48));
                var digitUpper = Vector.LessThanOrEqual(vec, new Vector<ushort>(57));
                var isDigit = Vector.BitwiseAnd(digitLower, digitUpper);
                int digitMatches = CountBitsSet(isDigit);
                

                // Letter check - simplified check for ASCII letters
                // (A-Z: 65-90, a-z: 97-122)
                var lowerALower = Vector.GreaterThanOrEqual(vec, new Vector<ushort>(65));
                var lowerAUpper = Vector.LessThanOrEqual(vec, new Vector<ushort>(90));
                var upperALower = Vector.GreaterThanOrEqual(vec, new Vector<ushort>(97));
                var upperAUpper = Vector.LessThanOrEqual(vec, new Vector<ushort>(122));
                var isLowerA = Vector.BitwiseAnd(lowerALower, lowerAUpper);
                var isUpperA = Vector.BitwiseAnd(upperALower, upperAUpper);
                var isLetter = Vector.BitwiseOr(isLowerA, isUpperA);
                int letterMatches = CountBitsSet(isLetter);                

                // Whitespace simplified check for common whitespace chars
                var isSpace = Vector.Equals(vec, new Vector<ushort>(' '));
                var isTab = Vector.Equals(vec, new Vector<ushort>('\t'));
                var isNewline = Vector.Equals(vec, new Vector<ushort>('\n'));
                var isReturn = Vector.Equals(vec, new Vector<ushort>('\r'));
                var isWhitespace = Vector.BitwiseOr(Vector.BitwiseOr(isSpace, isTab),
                                  Vector.BitwiseOr(isNewline, isReturn));
                int whitespaceMatches = CountBitsSet(isWhitespace);
                whitespaceCount += whitespaceMatches;

                // Control character check (0-31: 0-31, 127: 127)
                var controlLower = Vector.LessThan(vec, new Vector<ushort>(32));
                var isDel = Vector.Equals(vec, new Vector<ushort>(127));
                var isControl = Vector.BitwiseOr(controlLower, isDel);
                int controlMatches = CountBitsSet(isControl);
                controlCount += controlMatches;

                // Update flags
                if (letterMatches > 0) hasLetters = true;
                if (digitMatches > 0) hasDigits = true;

                // Check if onlyLetters or onlyDigits should be false
                if (onlyLetters && (digitMatches > 0 ||
                                    whitespaceMatches > 0 ||
                                    controlMatches > 0))
                {
                    onlyLetters = false;
                }

                if (onlyDigits && (letterMatches > 0 ||
                                  whitespaceMatches > 0 ||
                                  controlMatches > 0))
                {
                    onlyDigits = false;
                }
            }

            // Process remaining characters
            //for (; i < charSpan.Length; i++)
            //{
            //    char c = charSpan[i];
            //    if (char.IsLetter(c))
            //    {
            //        letterCount++;
            //        hasLetters = true;
            //        onlyDigits = false;
            //    }
            //    else if (char.IsDigit(c))
            //    {
            //        digitCount++;
            //        hasDigits = true;
            //        onlyLetters = false;
            //    }
            //    else if (char.IsWhiteSpace(c))
            //    {
            //        whitespaceCount++;
            //        onlyLetters = false;
            //        onlyDigits = false;
            //    }
            //    else if (char.IsPunctuation(c))
            //    {
            //        punctuationCount++;
            //        hasPunctuation = true;
            //        onlyLetters = false;
            //        onlyDigits = false;
            //    }
            //    else if (char.IsControl(c))
            //    {
            //        controlCount++;
            //        hasNonPrintable = true;
            //        onlyLetters = false;
            //        onlyDigits = false;
            //    }
            //    else
            //    {
            //        specialCount++;
            //        hasSpecialChars = true;
            //        onlyLetters = false;
            //        onlyDigits = false;
            //    }
            //}
            if (hasLetters)
            {
                letterCount++;
            }

            // If value has any digit
            if (hasDigits)
            {
                digitCount++;
            }

            // If value has any punctuation
            if (hasPunctuation)
            {
                punctuationCount++;
            }

            // if value has any non printable characters
            if (hasNonPrintable)
            {
                controlCount++;
            }

            // if value has any special characters
            if (hasSpecialChars)
            {
                specialCount++;
            }

            // Update character counts
            Interlocked.Add(ref _lettersCount, letterCount);
            Interlocked.Add(ref _digitsCount, digitCount);
            Interlocked.Add(ref _whitespaceCount, whitespaceCount);
            Interlocked.Add(ref _punctuationCount, punctuationCount);
            Interlocked.Add(ref _nonPrintableCount, controlCount);
            Interlocked.Add(ref _specialCharCount, specialCount);
        }
        else
        {
            // Fall back to regular character analysis
            AnalyzeCharacters(value, fullRow, rowNumber);
            return;
        }

        UpdateCharacteristicRows(value, fullRow, rowNumber, onlyLetters, onlyDigits, hasLetters, hasDigits,
            hasPunctuation, hasSpecialChars, hasNonPrintable);
    }

    /// <summary>
    /// Update characteristic rows for character analysis
    /// </summary>
    private void UpdateCharacteristicRows(
        string value,
        IDictionary<string, object> fullRow,
        long rowNumber,
        bool onlyLetters,
        bool onlyDigits,
        bool hasLetters,
        bool hasDigits,
        bool hasPunctuation,
        bool hasSpecialChars,
        bool hasNonPrintable)
    {
        // Update counts and store row references for character characteristics
        // Only update letters-only if we haven't already identified as numeric-only
        if (onlyLetters && hasLetters)
        {
            Interlocked.Increment(ref _lettersOnlyCount);
            if (_characteristicRows[ProfileCharacteristic.LettersOnly].Count < _options.MaxRowsPerCategory)
            {
                _characteristicRows[ProfileCharacteristic.LettersOnly].Add(
                    CreateRowReference(fullRow, value, rowNumber));
            }
        }

        // Only update digits-only if it wasn't already counted in numeric check
        if (onlyDigits && hasDigits && !double.TryParse(value, out _))
        {
            Interlocked.Increment(ref _digitsOnlyCount);
            if (_characteristicRows[ProfileCharacteristic.NumbersOnly].Count < _options.MaxRowsPerCategory)
            {
                _characteristicRows[ProfileCharacteristic.NumbersOnly].Add(
                    CreateRowReference(fullRow, value, rowNumber));
            }
        }

        if (hasLetters && hasDigits && !hasPunctuation && !hasSpecialChars)
        {
            Interlocked.Increment(ref _alphanumericCount);
            if (_characteristicRows[ProfileCharacteristic.Alphanumeric].Count < _options.MaxRowsPerCategory)
            {
                _characteristicRows[ProfileCharacteristic.Alphanumeric].Add(
                    CreateRowReference(fullRow, value, rowNumber));
            }
        }        

        if (hasSpecialChars)
        {
            if (_characteristicRows[ProfileCharacteristic.WithSpecialChars].Count < _options.MaxRowsPerCategory)
            {
                _characteristicRows[ProfileCharacteristic.WithSpecialChars].Add(
                    CreateRowReference(fullRow, value, rowNumber));
            }
        }

        if (hasNonPrintable)
        {
            if (_characteristicRows[ProfileCharacteristic.WithNonPrintable].Count < _options.MaxRowsPerCategory)
            {
                _characteristicRows[ProfileCharacteristic.WithNonPrintable].Add(
                    CreateRowReference(fullRow, value, rowNumber));
            }
        }
    }


    private int CountBitsSet<T>(Vector<T> vector) where T : struct
    {
        int count = 0;

        // Get the raw byte representation of the vector
        var tempBytes = new byte[Vector<T>.Count * Unsafe.SizeOf<T>()];
        MemoryMarshal.Write(tempBytes, vector);

        // Count set bits
        for (int i = 0; i < tempBytes.Length; i++)
        {
            byte b = tempBytes[i];
            // Count bits using Brian Kernighan's algorithm
            while (b != 0)
            {
                b &= (byte)(b - 1);
                count++;
            }
        }

        return count;
    }
}
