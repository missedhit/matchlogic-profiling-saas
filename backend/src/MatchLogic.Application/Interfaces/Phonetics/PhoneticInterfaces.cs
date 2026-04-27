using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.Phonetics;
public interface ITransliterator
{
    string Transliterate(string input);
}

public interface IPhoneticEncoder
{
    string Encode(string input);
    int MatchRating(string input1, string input2);
    bool IsSimilar(string input1, string input2);
}

public interface IPhoneticConverter
{
    string ConvertToPhonetic(string input);
    int MatchRating(string input1, string input2);
    bool IsSimilar(string input1, string input2);
    int MinimumRating(string input1, string input2);
}

