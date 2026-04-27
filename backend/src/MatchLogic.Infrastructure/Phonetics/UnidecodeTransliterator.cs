using MatchLogic.Application.Interfaces.Phonetics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Unidecode.NET;

namespace MatchLogic.Infrastructure.Phonetics;
public class UnidecodeTransliterator : ITransliterator
{
    public string Transliterate(string input)
    {
        return input.Unidecode();
    }
}
