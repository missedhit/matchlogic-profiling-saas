using MatchLogic.Domain.Dictionary;
using MatchLogic.Infrastructure.CleansingAndStandardization.Parser.GenericParser;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DictionaryCategory = MatchLogic.Infrastructure.CleansingAndStandardization.Parser.GenericParser.DictionaryCategory;

namespace MatchLogic.Infrastructure.CleansingAndStandardization.Parser.AddressParser
{
    public class AddressParser : GenericParser.GenericParser
    {
        #region Constants

        const int minCity = 2;

        #endregion

        #region Enums

        public enum RecognizedTypes
        {
            Zip,
            ZipPlus4,
            City,
            State,
            Country,
            GeoDirection,
            SecondaryUnitDesignator,
            StreetSuffix,
            OrdinalWord,
            NumberWord,
            PoBoxPrefix,
            RuralRoutePrefix,
            Box,
            Numeric4Digits,
            Numeric5Digits,
            Numeric,
            UndefinedLetters,
            AlphaNumeric,
            Hyphen,
            Slash,
            BackSlash,
            AlphaNumericAlpha, // 3 chars: first and third are letters and second is digit
            NumericAlphaNumeric, // 3 chars: first and third are digits and second is letter
            CanadianZip // 6 chars: ANANAN
        }

        /// <summary>
        /// this is very similar to RecognizedTypes, the difference is that PatternIds are one level higher and can be identified by many combinations of RecognizedTypes
        /// e.g. pattern PostalBox is recognized as combination of:
        /// PoBoxPrefix, Numeric or
        /// PoBoxPrefix, AlphaNumeric or
        /// RuralRoutePrefix, Numeric or
        /// RuralRoutePrefix, Numeric, Box, AlphaNumeric
        /// </summary>
        public enum PatternIds
        {
            RecipientPattern,
            CityPattern,
            ZipPattern,
            Zip9Pattern,
            StatePattern,
            CountryPattern,
            PostalBoxPattern,
            BoxPattern,
            StreetPattern,
            StreetNumberPattern,
            StreetSuffixPattern,
            SecondaryDesignatorPattern,
            GeoDirectionPattern,
            UndefinedPattern
        }

        #endregion

        #region Fields

        #region abbreviations

        internal StateCategory StatePossessionAbbreviations = new StateCategory((int)RecognizedTypes.State);
        private AbbreviationCategory countryAbbreviations = new AbbreviationCategory((int)RecognizedTypes.Country);

        private AbbreviationCategory geographicDirectionalAbbreviations =
            new AbbreviationCategory((int)RecognizedTypes.GeoDirection);

        private AbbreviationCategory secondaryUnitDesignators =
            new AbbreviationCategory((int)RecognizedTypes.SecondaryUnitDesignator);

        private AbbreviationCategory streetSuffixAbbreviations =
            new AbbreviationCategory((int)RecognizedTypes.StreetSuffix);

        private AbbreviationCategory ordinalWords = new AbbreviationCategory((int)RecognizedTypes.OrdinalWord);
        private AbbreviationCategory numberWords = new AbbreviationCategory((int)RecognizedTypes.NumberWord);

        #endregion

        #region simple dictionaries

        private DictionaryCategory postalBoxPrefixWords = new DictionaryCategory((int)RecognizedTypes.PoBoxPrefix);

        private DictionaryCategory ruralRoutePrefixWords =
            new DictionaryCategory((int)RecognizedTypes.RuralRoutePrefix);

        private DictionaryCategory boxWords = new DictionaryCategory((int)RecognizedTypes.Box);

        #endregion

        #region patternGroups


        PatternsGroup citiesPatternsGroup = new PatternsGroup((int)PatternIds.CityPattern
#if DEBUG
            , "Cities"
#endif
        );

        PatternsGroup countriesPatternsGroup = new PatternsGroup((int)PatternIds.CountryPattern
#if DEBUG
            , "Countries"
#endif
        );

        PatternsGroup usZip9PatternsGroup = new PatternsGroup((int)PatternIds.Zip9Pattern
#if DEBUG
            , "Zip 9"
#endif
        );

        PatternsGroup zipPatternsGroup = new PatternsGroup((int)PatternIds.ZipPattern
#if DEBUG
            , "Zip"
#endif
        );

        PatternsGroup canadianZipPatternsGroup = new PatternsGroup((int)PatternIds.ZipPattern
#if DEBUG
            , "Canadian Zip"
#endif
        );

        PatternsGroup geoDirectionPatternsGroup = new PatternsGroup((int)PatternIds.GeoDirectionPattern
#if DEBUG
            , "Direction"
#endif
        );

        PatternsGroup statePatternsGroup = new PatternsGroup((int)PatternIds.StatePattern
#if DEBUG
            , "State"
#endif
        );

        PatternsGroup postalBoxPatternsGroup = new PatternsGroup((int)PatternIds.PostalBoxPattern
#if DEBUG
            , "Postal Box"
#endif
        );

        PatternsGroup boxPatternsGroup = new PatternsGroup((int)PatternIds.BoxPattern
#if DEBUG
            , "Box"
#endif
        );

        PatternsGroup streetNumberPatternsGroup = new PatternsGroup((int)PatternIds.StreetNumberPattern
#if DEBUG
            , "Street Number"
#endif
        );

        PatternsGroup secondaryAddressUnitPatternsGroup = new PatternsGroup(
            (int)PatternIds.SecondaryDesignatorPattern
#if DEBUG
            , "Secondary Designator"
#endif
        );

        PatternsGroup streetSuffixPatternsGroup = new PatternsGroup((int)PatternIds.StreetSuffixPattern
#if DEBUG
            , "Street Suffix"
#endif
        );

        #endregion

        #region misc

        //private PredefinedStringTypes predefinedStringTypes;

        private int inputLinesCount = 0;

        #endregion

        private bool shouldEnrichStreet = false;

        #endregion

        #region Constructors

        public AddressParser()            
        {
            MaxWordsInCategory = 4;
            initDictionaries();
        }

        //public AddressParser(PredefinedStringTypes predefinedStringTypes)
        //    : base()
        //{
        //    this.predefinedStringTypes = predefinedStringTypes;
        //    this.predefinedStringTypes.StringDataTypeDeterminator =
        //        new StringDataTypeDeterminator(this.predefinedStringTypes);
        //    initDictionaries();
        //    this.predefinedStringTypes.LoadGeoDictionaries(this);
        //}

        #endregion

        #region Properties and Fields

        private ChoosenPattern city;

        public string City
        {
            get { return !isPatternNullOrRemoved(city) ? city.Text : ""; }
        }

        private ChoosenPattern recipient;

        public string Recipient
        {
            get { return !isPatternNullOrRemoved(recipient) ? recipient.Text : ""; }
        }

        private ChoosenPattern zipCode;

        public string ZipCode
        {
            get
            {
                string result = "";
                if (zipCode != null)
                {
                    result = zipCode.Text;
                }
                else if (Zip9Code != "")
                {
                    result = Zip9Code.Substring(0, 5);
                }

                return result;
            }
        }

        private ChoosenPattern zip9Code;

        public string Zip9Code
        {
            get { return !isPatternNullOrRemoved(zip9Code) ? zip9Code.Text.Replace(" ", "") : ""; }
        } // maybe to consider change of CategorizedGroupOfWordsList.ToString() ...

        private ChoosenPattern state;

        public string State
        {
            get
            {
                string result = "";
                if (!isPatternNullOrRemoved(state))
                {
                    result = StatePossessionAbbreviations.GetValue(state.Text);
                }

                return result;
            }
        }

        private ChoosenPattern street;

        public string Street
        {
            get
            {
                string result = "";
                if (!isPatternNullOrRemoved(street))
                {
                    result = street.Text;
                }


                return result;
            }
        }

        private ChoosenPattern streetSuffix;

        public string StreetSuffix
        {
            get
            {
                string result = "";
                if (!isPatternNullOrRemoved(streetSuffix))
                {
                    result = streetSuffixAbbreviations.GetValue(streetSuffix.Text);
                }

                return result;
            }
        }

        private ChoosenPattern streetNumber;

        public string StreetNumber
        {
            get
            {
                string result = "";
                if (!isPatternNullOrRemoved(streetNumber))
                {
                    if (!streetNumber.Removed)
                    {
                        string number = numberWords.GetValue(streetNumber.Text);
                        if (!string.IsNullOrEmpty(number))
                        {
                            result = number;
                        }
                        else
                        {
                            result = streetNumber.Text;
                        }
                    }
                }

                return result;
            }
        }

        private ChoosenPattern suite;

        public string Suite
        {
            get { return !isPatternNullOrRemoved(suite) ? suite.Text : ""; }
        }

        private ChoosenPattern preDirection;

        public string PreDirection
        {
            get
            {
                string result = "";
                if (!isPatternNullOrRemoved(preDirection))
                {
                    result = geographicDirectionalAbbreviations.GetValue(preDirection.Text);
                }

                return result;
            }
        }

        private ChoosenPattern postDirection;

        public string PostDirection
        {
            get
            {
                string result = "";
                if (!isPatternNullOrRemoved(postDirection))
                {
                    result = geographicDirectionalAbbreviations.GetValue(postDirection.Text);
                }

                return result;
            }
        }

        public string Address
        {
            get
            {
                StringBuilder result = new StringBuilder();
                result.Append(StreetNumber);
                result.Append(' ');
                if (PreDirection.Length > 0)
                {
                    result.Append(PreDirection);
                    result.Append(' ');
                }

                if (Street.Length > 0)
                {
                    result.Append(Street);
                    result.Append(' ');
                }

                if (PostDirection.Length > 0)
                {
                    result.Append(PostDirection);
                    result.Append(' ');
                }

                return result.ToString();
            }
        }

        /// <summary>
        /// e.g. PO BOX 2345A
        /// </summary>
        private ChoosenPattern poBoxComplete;

        public string PoBoxComplete
        {
            get { return !isPatternNullOrRemoved(poBoxComplete) ? poBoxComplete.Text : ""; }
        }

        private ChoosenPattern boxComplete;

        /// <summary>
        /// e.g. SUITE 120 BOX 23
        /// </summary>
        public string BoxComplete
        {
            get { return !isPatternNullOrRemoved(boxComplete) ? boxComplete.Text : ""; }
        }

        private ChoosenPattern secondaryAddressUnitComplete;

        public string SecondaryAddressUnitComplete
        {
            get
            {
                return !isPatternNullOrRemoved(secondaryAddressUnitComplete) ? secondaryAddressUnitComplete.Text : "";
            }
        }

        private string poBox;

        public string PoBox
        {
            get { return poBox; }
        }

        private string poBoxNumber;

        public string PoBoxNumber
        {
            get
            {
                string result = "";
                if (!string.IsNullOrEmpty(poBoxNumber))
                {
                    result = poBoxNumber;
                }

                if (!string.IsNullOrEmpty(boxNumber))
                {
                    result = boxNumber;
                }

                return result;
            }
        }

        private string box;

        public string Box
        {
            get
            {
                string result = "";
                if (!string.IsNullOrEmpty(box))
                {
                    result = box;
                }
                else if (!string.IsNullOrEmpty(poBox))
                {
                    result = "PO Box";
                }

                return result;
            }
        }

        private string boxNumber;

        public string BoxNumber
        {
            get { return PoBoxNumber; } // it is the same thing on the end...
        }

        private string secondaryAddressUnit;

        public string SecondaryAddressUnit
        {
            get
            {
                string result = "";
                if (secondaryAddressUnit != null)
                {
                    result = secondaryUnitDesignators.GetValue(secondaryAddressUnit);
                }

                return result;
            }
        }

        private string secondaryAddressUnitNumber;

        public string SecondaryAddressUnitNumber
        {
            get { return secondaryAddressUnitNumber; }
        }

        private string countryDterminedFromOtherAttributes = null;

        private ChoosenPattern country;

        public string Country
        {
            get
            {
                string result = "";
                if (countryDterminedFromOtherAttributes != null)
                {
                    result = countryDterminedFromOtherAttributes;
                }
                else if (!isPatternNullOrRemoved(country))
                {
                    result = country.Text;
                }
                else
                {
                    result = StatePossessionAbbreviations.TryGetCountry(State);
                }

                return result;
            }
        }

        #endregion

        #region Methods

        #region initialization

        private void initDictionaries()
        {
            initStatePossessionAbbreviations();
            initCountriesAbbreviations();
            initGeographicDirectionalAbbreviations();
            initSecondaryUnitDesignators();
            initUspsAbbreviations();
            initOrdinalWords();
            initNumberWords();
            initPostalBoxWords();
            initBoxWords();
            initRuralRouteWords();
        }

        private void initCountriesAbbreviations()
        {
            countryAbbreviations.AddPair("Afghanistan", "AFG");
            countryAbbreviations.AddPair("Netherlands Antilles", "AHO");
            countryAbbreviations.AddPair("Albania", "ALB");
            countryAbbreviations.AddPair("Algeria", "ALG");
            countryAbbreviations.AddPair("Andorra", "AND");
            countryAbbreviations.AddPair("Angola", "ANG");
            countryAbbreviations.AddPair("Antigua and Barbuda", "ANT");
            countryAbbreviations.AddPair("Argentina", "ARG");
            countryAbbreviations.AddPair("Armenia", "ARM");
            countryAbbreviations.AddPair("Aruba", "ARU");
            countryAbbreviations.AddPair("American Samoa", "ASA");
            countryAbbreviations.AddPair("Australia", "AUS");
            countryAbbreviations.AddPair("Austria", "AUT");
            countryAbbreviations.AddPair("Azerbaijan", "AZE");
            countryAbbreviations.AddPair("Bahamas", "BAH");
            countryAbbreviations.AddPair("Bangladesh", "BAN");
            countryAbbreviations.AddPair("Barbados", "BAR");
            countryAbbreviations.AddPair("Burundi", "BDI");
            countryAbbreviations.AddPair("Belgium", "BEL");
            countryAbbreviations.AddPair("Benin", "BEN");
            countryAbbreviations.AddPair("Bermuda", "BER");
            countryAbbreviations.AddPair("Bhutan", "BHU");
            countryAbbreviations.AddPair("Bosnia and Herzegovina", "BIH");
            countryAbbreviations.AddPair("Belize", "BIZ");
            countryAbbreviations.AddPair("Belarus", "BLR");
            countryAbbreviations.AddPair("Bolivia", "BOL");
            countryAbbreviations.AddPair("Botswana", "BOT");
            countryAbbreviations.AddPair("Brazil", "BRA");
            countryAbbreviations.AddPair("Bahrain", "BRN");
            countryAbbreviations.AddPair("Brunei", "BRU");
            countryAbbreviations.AddPair("Bulgaria", "BUL");
            countryAbbreviations.AddPair("Burkina Faso", "BUR");
            countryAbbreviations.AddPair("Central African Republic", "CAF");
            countryAbbreviations.AddPair("Cambodia", "CAM");
            countryAbbreviations.AddPair("Canada", "CAN");
            countryAbbreviations.AddPair("Cayman Islands", "CAY");
            countryAbbreviations.AddPair("Congo", "CGO");
            countryAbbreviations.AddPair("Chad", "CHA");
            countryAbbreviations.AddPair("Chile", "CHI");
            countryAbbreviations.AddPair("China", "CHN");
            countryAbbreviations.AddPair("Côte d'Ivoire", "CIV");
            countryAbbreviations.AddPair("Cameroon", "CMR");
            countryAbbreviations.AddPair("DR Congo", "COD");
            countryAbbreviations.AddPair("Cook Islands", "COK");
            countryAbbreviations.AddPair("Colombia", "COL");
            countryAbbreviations.AddPair("Comoros", "COM");
            countryAbbreviations.AddPair("Cape Verde", "CPV");
            countryAbbreviations.AddPair("Costa Rica", "CRC");
            countryAbbreviations.AddPair("Croatia", "CRO");
            countryAbbreviations.AddPair("Cuba", "CUB");
            countryAbbreviations.AddPair("Cyprus", "CYP");
            countryAbbreviations.AddPair("Czech Republic", "CZE");
            countryAbbreviations.AddPair("Denmark", "DEN");
            countryAbbreviations.AddPair("Djibouti", "DJI");
            countryAbbreviations.AddPair("Dominica", "DMA");
            countryAbbreviations.AddPair("Dominican Republic", "DOM");
            countryAbbreviations.AddPair("Ecuador", "ECU");
            countryAbbreviations.AddPair("Egypt", "EGY");
            countryAbbreviations.AddPair("Eritrea", "ERI");
            countryAbbreviations.AddPair("El Salvador", "ESA");
            countryAbbreviations.AddPair("Spain", "ESP");
            countryAbbreviations.AddPair("Estonia", "EST");
            countryAbbreviations.AddPair("Ethiopia", "ETH");
            countryAbbreviations.AddPair("Fiji", "FIJ");
            countryAbbreviations.AddPair("Finland", "FIN");
            countryAbbreviations.AddPair("France", "FRA");
            countryAbbreviations.AddPair("Micronesia", "FSM");
            countryAbbreviations.AddPair("Gabon", "GAB");
            countryAbbreviations.AddPair("Gambia", "GAM");
            countryAbbreviations.AddPair("Great Britain", "GBR");
            countryAbbreviations.AddPair("Guinea-Bissau", "GBS");
            countryAbbreviations.AddPair("Georgia", "GEO");
            countryAbbreviations.AddPair("Equatorial Guinea", "GEQ");
            countryAbbreviations.AddPair("Germany", "GER");
            countryAbbreviations.AddPair("Ghana", "GHA");
            countryAbbreviations.AddPair("Greece", "GRE");
            countryAbbreviations.AddPair("Grenada", "GRN");
            countryAbbreviations.AddPair("Guatemala", "GUA");
            countryAbbreviations.AddPair("Guinea", "GUI");
            countryAbbreviations.AddPair("Guam", "GUM");
            countryAbbreviations.AddPair("Guyana", "GUY");
            countryAbbreviations.AddPair("Haiti", "HAI");
            countryAbbreviations.AddPair("Hong Kong", "HKG");
            countryAbbreviations.AddPair("Honduras", "HON");
            countryAbbreviations.AddPair("Hungary", "HUN");
            countryAbbreviations.AddPair("Indonesia", "INA");
            countryAbbreviations.AddPair("India", "IND");
            countryAbbreviations.AddPair("Iran", "IRI");
            countryAbbreviations.AddPair("Ireland", "IRL");
            countryAbbreviations.AddPair("Iraq", "IRQ");
            countryAbbreviations.AddPair("Iceland", "ISL");
            countryAbbreviations.AddPair("Israel", "ISR");
            countryAbbreviations.AddPair("Virgin Islands", "ISV");
            countryAbbreviations.AddPair("Italy", "ITA");
            countryAbbreviations.AddPair("British Virgin Islands", "IVB");
            countryAbbreviations.AddPair("Jamaica", "JAM");
            countryAbbreviations.AddPair("Jordan", "JOR");
            countryAbbreviations.AddPair("Japan", "JPN");
            countryAbbreviations.AddPair("Kazakhstan", "KAZ");
            countryAbbreviations.AddPair("Kenya", "KEN");
            countryAbbreviations.AddPair("Kyrgyzstan", "KGZ");
            countryAbbreviations.AddPair("Kiribati", "KIR");
            countryAbbreviations.AddPair("South Korea", "KOR");
            countryAbbreviations.AddPair("Saudi Arabia", "KSA");
            countryAbbreviations.AddPair("Kuwait", "KUW");
            countryAbbreviations.AddPair("Laos", "LAO");
            countryAbbreviations.AddPair("Latvia", "LAT");
            countryAbbreviations.AddPair("Libya", "LBA");
            countryAbbreviations.AddPair("Liberia", "LBR");
            countryAbbreviations.AddPair("Saint Lucia", "LCA");
            countryAbbreviations.AddPair("Lesotho", "LES");
            countryAbbreviations.AddPair("Lebanon", "LIB");
            countryAbbreviations.AddPair("Liechtenstein", "LIE");
            countryAbbreviations.AddPair("Lithuania", "LTU");
            countryAbbreviations.AddPair("Luxembourg", "LUX");
            countryAbbreviations.AddPair("Madagascar", "MAD");
            countryAbbreviations.AddPair("Morocco", "MAR");
            countryAbbreviations.AddPair("Malaysia", "MAS");
            countryAbbreviations.AddPair("Malawi", "MAW");
            countryAbbreviations.AddPair("Moldova", "MDA");
            countryAbbreviations.AddPair("Maldives", "MDV");
            countryAbbreviations.AddPair("Mexico", "MEX");
            countryAbbreviations.AddPair("Mongolia", "MGL");
            countryAbbreviations.AddPair("Marshall Islands", "MHL");
            countryAbbreviations.AddPair("Macedonia", "MKD");
            countryAbbreviations.AddPair("Mali", "MLI");
            countryAbbreviations.AddPair("Malta", "MLT");
            countryAbbreviations.AddPair("Montenegro", "MNE");
            countryAbbreviations.AddPair("Monaco", "MON");
            countryAbbreviations.AddPair("Mozambique", "MOZ");
            countryAbbreviations.AddPair("Mauritius", "MRI");
            countryAbbreviations.AddPair("Mauritania", "MTN");
            countryAbbreviations.AddPair("Myanmar", "MYA");
            countryAbbreviations.AddPair("Namibia", "NAM");
            countryAbbreviations.AddPair("Nicaragua", "NCA");
            countryAbbreviations.AddPair("Netherlands", "NED");
            countryAbbreviations.AddPair("Nepal", "NEP");
            countryAbbreviations.AddPair("Nigeria", "NGR");
            countryAbbreviations.AddPair("Niger", "NIG");
            countryAbbreviations.AddPair("Norway", "NOR");
            countryAbbreviations.AddPair("Nauru", "NRU");
            countryAbbreviations.AddPair("New Zealand", "NZL");
            countryAbbreviations.AddPair("Oman", "OMA");
            countryAbbreviations.AddPair("Pakistan", "PAK");
            countryAbbreviations.AddPair("Panama", "PAN");
            countryAbbreviations.AddPair("Paraguay", "PAR");
            countryAbbreviations.AddPair("Peru", "PER");
            countryAbbreviations.AddPair("Philippines", "PHI");
            countryAbbreviations.AddPair("Palestine", "PLE");
            countryAbbreviations.AddPair("Palau", "PLW");
            countryAbbreviations.AddPair("Papua New Guinea", "PNG");
            countryAbbreviations.AddPair("Poland", "POL");
            countryAbbreviations.AddPair("Portugal", "POR");
            countryAbbreviations.AddPair("North Korea", "PRK");
            countryAbbreviations.AddPair("Puerto Rico", "PUR");
            countryAbbreviations.AddPair("Qatar", "QAT");
            countryAbbreviations.AddPair("Romania", "ROU");
            countryAbbreviations.AddPair("South Africa", "RSA");
            countryAbbreviations.AddPair("Russia", "RUS");
            countryAbbreviations.AddPair("Rwanda", "RWA");
            countryAbbreviations.AddPair("Samoa", "SAM");
            countryAbbreviations.AddPair("Senegal", "SEN");
            countryAbbreviations.AddPair("Seychelles", "SEY");
            countryAbbreviations.AddPair("Singapore", "SIN");
            countryAbbreviations.AddPair("Saint Kitts and Nevis", "SKN");
            countryAbbreviations.AddPair("Sierra Leone", "SLE");
            countryAbbreviations.AddPair("Slovenia", "SLO");
            countryAbbreviations.AddPair("San Marino", "SMR");
            countryAbbreviations.AddPair("Solomon Islands", "SOL");
            countryAbbreviations.AddPair("Somalia", "SOM");
            countryAbbreviations.AddPair("Serbia", "SRB");
            countryAbbreviations.AddPair("Sri Lanka", "SRI");
            countryAbbreviations.AddPair("São Tomé and Príncipe", "STP");
            countryAbbreviations.AddPair("Sudan", "SUD");
            countryAbbreviations.AddPair("Switzerland", "SUI");
            countryAbbreviations.AddPair("Suriname", "SUR");
            countryAbbreviations.AddPair("Slovakia", "SVK");
            countryAbbreviations.AddPair("Sweden", "SWE");
            countryAbbreviations.AddPair("Swaziland", "SWZ");
            countryAbbreviations.AddPair("Syria", "SYR");
            countryAbbreviations.AddPair("Tanzania", "TAN");
            countryAbbreviations.AddPair("Tonga", "TGA");
            countryAbbreviations.AddPair("Thailand", "THA");
            countryAbbreviations.AddPair("Tajikistan", "TJK");
            countryAbbreviations.AddPair("Turkmenistan", "TKM");
            countryAbbreviations.AddPair("Timor-Leste", "TLS");
            countryAbbreviations.AddPair("Togo", "TOG");
            countryAbbreviations.AddPair("Chinese Taipei", "TPE");
            countryAbbreviations.AddPair("Trinidad and Tobago", "TRI");
            countryAbbreviations.AddPair("Tunisia", "TUN");
            countryAbbreviations.AddPair("Turkey", "TUR");
            countryAbbreviations.AddPair("Tuvalu", "TUV");
            countryAbbreviations.AddPair("United Arab Emirates", "UAE");
            countryAbbreviations.AddPair("Uganda", "UGA");
            countryAbbreviations.AddPair("Ukraine", "UKR");
            countryAbbreviations.AddPair("Uruguay", "URU");
            countryAbbreviations.AddPair("United States", "USA");
            countryAbbreviations.AddPair("Uzbekistan", "UZB");
            countryAbbreviations.AddPair("Vanuatu", "VAN");
            countryAbbreviations.AddPair("Venezuela", "VEN");
            countryAbbreviations.AddPair("Vietnam", "VIE");
            countryAbbreviations.AddPair("Saint Vincent and the Grenadines", "VIN");
            countryAbbreviations.AddPair("Yemen", "YEM");
            countryAbbreviations.AddPair("Zambia", "ZAM");
            countryAbbreviations.AddPair("Zimbabwe", "ZIM");
        }

        private void initStatePossessionAbbreviations()
        {
            if (StatePossessionAbbreviations.Count == 0
            ) // because statePossessionAbbreviations is static - it is small and needed elsewhere...
            {
                string country = "USA";
                StatePossessionAbbreviations.AddPairPlusCountry("ALABAMA", "AL", country);
                StatePossessionAbbreviations.AddPairPlusCountry("ALASKA", "AK", country);
                StatePossessionAbbreviations.AddPairPlusCountry("AMERICAN SAMOA", "AS", country);
                StatePossessionAbbreviations.AddPairPlusCountry("ARIZONA", "AZ", country);
                StatePossessionAbbreviations.AddPairPlusCountry("ARKANSAS", "AR", country);
                StatePossessionAbbreviations.AddPairPlusCountry("CALIFORNIA", "CA", country);
                StatePossessionAbbreviations.AddPairPlusCountry("COLORADO", "CO", country);
                StatePossessionAbbreviations.AddPairPlusCountry("CONNECTICUT", "CT", country);
                StatePossessionAbbreviations.AddPairPlusCountry("DELAWARE", "DE", country);
                StatePossessionAbbreviations.AddPairPlusCountry("DISTRICT OF COLUMBIA", "DC", country);
                StatePossessionAbbreviations.AddPairPlusCountry("FEDERATED STATES OF MICRONESIA", "FM", country);
                StatePossessionAbbreviations.AddPairPlusCountry("FLORIDA", "FL", country);
                StatePossessionAbbreviations.AddPairPlusCountry("GEORGIA", "GA", country);
                StatePossessionAbbreviations.AddPairPlusCountry("GUAM", "GU", country);
                StatePossessionAbbreviations.AddPairPlusCountry("HAWAII", "HI", country);
                StatePossessionAbbreviations.AddPairPlusCountry("IDAHO", "ID", country);
                StatePossessionAbbreviations.AddPairPlusCountry("ILLINOIS", "IL", country);
                StatePossessionAbbreviations.AddPairPlusCountry("INDIANA", "IN", country);
                StatePossessionAbbreviations.AddPairPlusCountry("IOWA", "IA", country);
                StatePossessionAbbreviations.AddPairPlusCountry("KANSAS", "KS", country);
                StatePossessionAbbreviations.AddPairPlusCountry("KENTUCKY", "KY", country);
                StatePossessionAbbreviations.AddPairPlusCountry("LOUISIANA", "LA", country);
                StatePossessionAbbreviations.AddPairPlusCountry("MAINE", "ME", country);
                StatePossessionAbbreviations.AddPairPlusCountry("MARSHALL ISLANDS", "MH", country);
                StatePossessionAbbreviations.AddPairPlusCountry("MARYLAND", "MD", country);
                StatePossessionAbbreviations.AddPairPlusCountry("MASSACHUSETTS", "MA", country);
                StatePossessionAbbreviations.AddPairPlusCountry("MICHIGAN", "MI", country);
                StatePossessionAbbreviations.AddPairPlusCountry("MINNESOTA", "MN", country);
                StatePossessionAbbreviations.AddPairPlusCountry("MISSISSIPPI", "MS", country);
                StatePossessionAbbreviations.AddPairPlusCountry("MISSOURI", "MO", country);
                StatePossessionAbbreviations.AddPairPlusCountry("MONTANA", "MT", country);
                StatePossessionAbbreviations.AddPairPlusCountry("NEBRASKA", "NE", country);
                StatePossessionAbbreviations.AddPairPlusCountry("NEVADA", "NV", country);
                StatePossessionAbbreviations.AddPairPlusCountry("NEW HAMPSHIRE", "NH", country);
                StatePossessionAbbreviations.AddPairPlusCountry("NEW JERSEY", "NJ", country);
                StatePossessionAbbreviations.AddPairPlusCountry("NEW MEXICO", "NM", country);
                StatePossessionAbbreviations.AddPairPlusCountry("NEW YORK", "NY", country);
                StatePossessionAbbreviations.AddPairPlusCountry("NORTH CAROLINA", "NC", country);
                StatePossessionAbbreviations.AddPairPlusCountry("NORTH DAKOTA", "ND", country);
                StatePossessionAbbreviations.AddPairPlusCountry("NORTHERN MARIANA ISLANDS", "MP", country);
                StatePossessionAbbreviations.AddPairPlusCountry("OHIO", "OH", country);
                StatePossessionAbbreviations.AddPairPlusCountry("OKLAHOMA", "OK", country);
                StatePossessionAbbreviations.AddPairPlusCountry("OREGON", "OR", country);
                StatePossessionAbbreviations.AddPairPlusCountry("PALAU", "PW", country);
                StatePossessionAbbreviations.AddPairPlusCountry("PENNSYLVANIA", "PA", country);
                StatePossessionAbbreviations.AddPairPlusCountry("PUERTO RICO", "PR", country);
                StatePossessionAbbreviations.AddPairPlusCountry("RHODE ISLAND", "RI", country);
                StatePossessionAbbreviations.AddPairPlusCountry("SOUTH CAROLINA", "SC", country);
                StatePossessionAbbreviations.AddPairPlusCountry("SOUTH DAKOTA", "SD", country);
                StatePossessionAbbreviations.AddPairPlusCountry("TENNESSEE", "TN", country);
                StatePossessionAbbreviations.AddPairPlusCountry("TEXAS", "TX", country);
                StatePossessionAbbreviations.AddPairPlusCountry("UTAH", "UT", country);
                StatePossessionAbbreviations.AddPairPlusCountry("VERMONT", "VT", country);
                StatePossessionAbbreviations.AddPairPlusCountry("VIRGIN ISLANDS", "VI", country);
                StatePossessionAbbreviations.AddPairPlusCountry("VIRGINIA", "VA", country);
                StatePossessionAbbreviations.AddPairPlusCountry("WASHINGTON", "WA", country);
                StatePossessionAbbreviations.AddPairPlusCountry("WEST VIRGINIA", "WV", country);
                StatePossessionAbbreviations.AddPairPlusCountry("WISCONSIN", "WI", country);
                StatePossessionAbbreviations.AddPairPlusCountry("WYOMING", "WY", country);

                country = "Canada";
                StatePossessionAbbreviations.AddPairPlusCountry("ONTARIO", "ON", country);
                StatePossessionAbbreviations.AddPairPlusCountry("QUEBEC", "QC", country);
                StatePossessionAbbreviations.AddPairPlusCountry("BRITISH COLUMBIA", "BC", country);
                StatePossessionAbbreviations.AddPairPlusCountry("ALBERTA", "AB", country);
                StatePossessionAbbreviations.AddPairPlusCountry("MANITOBA", "MB", country);
                StatePossessionAbbreviations.AddPairPlusCountry("SASKATCHEWAN", "SK", country);
                StatePossessionAbbreviations.AddPairPlusCountry("NOVA SCOTIA", "NS", country);
                StatePossessionAbbreviations.AddPairPlusCountry("NEW BRUNSWICK", "NB", country);
                StatePossessionAbbreviations.AddPairPlusCountry("NEWFOUNDLAND AND LABRADOR", "NL", country);
                StatePossessionAbbreviations.AddPairPlusCountry("PRINCE EDWARD ISLAND", "PE", country);
                StatePossessionAbbreviations.AddPairPlusCountry("NORTHWEST TERRITORIES", "NT", country);
                StatePossessionAbbreviations.AddPairPlusCountry("YUKON", "YT", country);
                StatePossessionAbbreviations.AddPairPlusCountry("NUNAVUT", "NU", country);

                // UK
                country = "Channel Islands";
                StatePossessionAbbreviations.AddPairPlusCountry("ALD", "Alderney", country);
                StatePossessionAbbreviations.AddPairPlusCountry("GSY", "Guernsey", country);
                StatePossessionAbbreviations.AddPairPlusCountry("JSY", "Jersey", country);
                StatePossessionAbbreviations.AddPairPlusCountry("SRK", "Sark", country);

                country = "England";
                StatePossessionAbbreviations.AddPairPlusCountry("SRY", "Surrey", country);
                StatePossessionAbbreviations.AddPairPlusCountry("SSX", "Sussex", country);
                StatePossessionAbbreviations.AddPairPlusCountry("SOM", "Somerset", country);
                StatePossessionAbbreviations.AddPairPlusCountry("STS", "Staffordshire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("RUT", "Rutland", country);
                StatePossessionAbbreviations.AddPairPlusCountry("SAL", "Shropshire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("SFK", "Suffolk", country);
                StatePossessionAbbreviations.AddPairPlusCountry("SXE", "East Sussex", country);
                StatePossessionAbbreviations.AddPairPlusCountry("SXW", "West Sussex", country);
                StatePossessionAbbreviations.AddPairPlusCountry("SYK", "South Yorkshire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("TWR", "Tyne and Wear", country);
                StatePossessionAbbreviations.AddPairPlusCountry("WAR", "Warwickshire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("WES", "Westmorland", country);
                StatePossessionAbbreviations.AddPairPlusCountry("WMD", "West Midlands", country);
                StatePossessionAbbreviations.AddPairPlusCountry("WOR", "Worcestershire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("WRY", "West Riding of Yorkshire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("WYK", "West Yorkshire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("YKS", "Yorkshire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("LEI", "Leicestershire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("LIN", "Lincolnshire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("IOW", "Isle of Wight", country);
                StatePossessionAbbreviations.AddPairPlusCountry("NBL", "Northumberland", country);
                StatePossessionAbbreviations.AddPairPlusCountry("NFK", "Norfolk", country);
                StatePossessionAbbreviations.AddPairPlusCountry("NRY", "North Riding of Yorkshire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("NTH", "Northamptonshire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("NTT", "Nottinghamshire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("NYK", "North Yorkshire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("MSY", "Merseyside", country);
                StatePossessionAbbreviations.AddPairPlusCountry("OXF", "Oxfordshire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("LAN", "Lancashire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("KEN", "Kent", country);
                StatePossessionAbbreviations.AddPairPlusCountry("GTM", "Greater Manchester", country);
                StatePossessionAbbreviations.AddPairPlusCountry("HAM", "Hampshire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("HEF", "Herefordshire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("HRT", "Hertfordshire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("HUM", "Humberside", country);
                StatePossessionAbbreviations.AddPairPlusCountry("HUN", "Huntingdonshire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("HWR", "Hereford and Worcester", country);
                StatePossessionAbbreviations.AddPairPlusCountry("DOR", "Dorset", country);
                StatePossessionAbbreviations.AddPairPlusCountry("DUR", "Co. Durham", country);
                StatePossessionAbbreviations.AddPairPlusCountry("ERY", "East Riding of Yorkshire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("ESS", "Essex", country);
                StatePossessionAbbreviations.AddPairPlusCountry("GLS", "Gloucestershire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("AVN", "Avon", country);
                StatePossessionAbbreviations.AddPairPlusCountry("BDF", "Bedfordshire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("BKM", "Buckinghamshire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("BRK", "Berkshire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("CAM", "Cambridgeshire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("CHS", "Cheshire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("CLV", "Cleveland", country);
                StatePossessionAbbreviations.AddPairPlusCountry("CMA", "Cumbria", country);
                StatePossessionAbbreviations.AddPairPlusCountry("CON", "Cornwall", country);
                StatePossessionAbbreviations.AddPairPlusCountry("CUL", "Cumberland", country);
                StatePossessionAbbreviations.AddPairPlusCountry("DBY", "Derbyshire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("DEV", "Devon", country);
                StatePossessionAbbreviations.AddPairPlusCountry("WIL", "Wiltshire", country);

                country = "Ireland";
                StatePossessionAbbreviations.AddPairPlusCountry("TIP", "Co. Tipperary", country);
                StatePossessionAbbreviations.AddPairPlusCountry("COR", "Co. Cork", country);
                StatePossessionAbbreviations.AddPairPlusCountry("CLA", "Co. Clare", country);
                StatePossessionAbbreviations.AddPairPlusCountry("CAR", "Co. Carlow", country);
                StatePossessionAbbreviations.AddPairPlusCountry("CAV", "Co. Cavan", country);
                StatePossessionAbbreviations.AddPairPlusCountry("GAL", "Co. Galway", country);
                StatePossessionAbbreviations.AddPairPlusCountry("DON", "Co. Donegal", country);
                StatePossessionAbbreviations.AddPairPlusCountry("DUB", "Co. Dublin", country);
                StatePossessionAbbreviations.AddPairPlusCountry("KER", "Co. Kerry", country);
                StatePossessionAbbreviations.AddPairPlusCountry("KID", "Co. Kildare", country);
                StatePossessionAbbreviations.AddPairPlusCountry("KIK", "Co. Kilkenny", country);
                StatePossessionAbbreviations.AddPairPlusCountry("OFF", "Co. Offaly", country);
                StatePossessionAbbreviations.AddPairPlusCountry("MOG", "Co. Monaghan", country);
                StatePossessionAbbreviations.AddPairPlusCountry("MAY", "Co. Mayo", country);
                StatePossessionAbbreviations.AddPairPlusCountry("MEA", "Co. Meath", country);
                StatePossessionAbbreviations.AddPairPlusCountry("LOG", "Co. Longford", country);
                StatePossessionAbbreviations.AddPairPlusCountry("LOU", "Co. Louth", country);
                StatePossessionAbbreviations.AddPairPlusCountry("LET", "Co. Leitrim", country);
                StatePossessionAbbreviations.AddPairPlusCountry("LEX", "Co. Laois", country);
                StatePossessionAbbreviations.AddPairPlusCountry("LIM", "Co. Limerick", country);
                StatePossessionAbbreviations.AddPairPlusCountry("WEX", "Co. Wexford", country);
                StatePossessionAbbreviations.AddPairPlusCountry("WIC", "Co. Wicklow", country);
                StatePossessionAbbreviations.AddPairPlusCountry("WAT", "Co. Waterford", country);
                StatePossessionAbbreviations.AddPairPlusCountry("WEM", "Co. Westmeath", country);
                StatePossessionAbbreviations.AddPairPlusCountry("ROS", "Co. Roscommon", country);
                StatePossessionAbbreviations.AddPairPlusCountry("SLI", "Co. Sligo", country);

                country = "Northern Ireland";
                StatePossessionAbbreviations.AddPairPlusCountry("TYR", "Co. Tyrone", country);
                StatePossessionAbbreviations.AddPairPlusCountry("LDY", "Co. Londonderry", country);
                StatePossessionAbbreviations.AddPairPlusCountry("DOW", "Co. Down", country);
                StatePossessionAbbreviations.AddPairPlusCountry("FER", "Co. Fermanagh", country);
                StatePossessionAbbreviations.AddPairPlusCountry("ARM", "Co. Armagh", country);
                StatePossessionAbbreviations.AddPairPlusCountry("ANT", "Co. Antrim", country);

                country = "Scotland";
                StatePossessionAbbreviations.AddPairPlusCountry("ARL", "Argyllshire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("AYR", "Ayrshire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("BAN", "Banffshire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("ANS", "Angus", country);
                StatePossessionAbbreviations.AddPairPlusCountry("ABD", "Aberdeenshire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("CAI", "Caithness", country);
                StatePossessionAbbreviations.AddPairPlusCountry("BUT", "Bute", country);
                StatePossessionAbbreviations.AddPairPlusCountry("BOR", "Borders", country);
                StatePossessionAbbreviations.AddPairPlusCountry("BEW", "Berwickshire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("CEN", "Central", country);
                StatePossessionAbbreviations.AddPairPlusCountry("CLK", "Clackmannanshire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("FIF", "Fife", country);
                StatePossessionAbbreviations.AddPairPlusCountry("GMP", "Grampian", country);
                StatePossessionAbbreviations.AddPairPlusCountry("DFS", "Dumfries-shire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("DGY", "Dumfries and Galloway", country);
                StatePossessionAbbreviations.AddPairPlusCountry("DNB", "Dunbartonshire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("ELN", "East Lothian", country);
                StatePossessionAbbreviations.AddPairPlusCountry("KCD", "Kincardineshire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("KKD", "Kirkcudbrightshire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("KRS", "Kinross-shire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("INV", "Inverness-shire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("HLD", "Highland", country);
                StatePossessionAbbreviations.AddPairPlusCountry("TAY", "Tayside", country);
                StatePossessionAbbreviations.AddPairPlusCountry("WIG", "Wigtownshire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("SUT", "Sutherland", country);
                StatePossessionAbbreviations.AddPairPlusCountry("STD", "Strathclyde", country);
                StatePossessionAbbreviations.AddPairPlusCountry("STI", "Stirlingshire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("ROX", "Roxburghshire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("SEL", "Selkirkshire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("MOR", "Morayshire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("SHI", "Shetland", country);
                StatePossessionAbbreviations.AddPairPlusCountry("LTN", "Lothian", country);
                StatePossessionAbbreviations.AddPairPlusCountry("LKS", "Lanarkshire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("MLN", "Midlothian", country);
                StatePossessionAbbreviations.AddPairPlusCountry("OKI", "Orkney", country);
                StatePossessionAbbreviations.AddPairPlusCountry("PEE", "Peebles-shire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("NAI", "Nairn", country);
                StatePossessionAbbreviations.AddPairPlusCountry("PER", "Perth", country);
                StatePossessionAbbreviations.AddPairPlusCountry("RFW", "Renfrewshire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("ROC", "Ross and Cromarty", country);
                StatePossessionAbbreviations.AddPairPlusCountry("WIS", "Western Isles", country);
                StatePossessionAbbreviations.AddPairPlusCountry("WLN", "West Lothian", country);
                StatePossessionAbbreviations.AddPairPlusCountry("POW", "Powys", country);

                country = "Wales";
                StatePossessionAbbreviations.AddPairPlusCountry("RAD", "Radnorshire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("PEM", "Pembrokeshire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("MON", "Monmouthshire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("MER", "Merionethshire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("MGM", "Mid Glamorgan", country);
                StatePossessionAbbreviations.AddPairPlusCountry("MGY", "Montgomeryshire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("SGM", "South Glamorgan", country);
                StatePossessionAbbreviations.AddPairPlusCountry("WGM", "West Glamorgan", country);
                StatePossessionAbbreviations.AddPairPlusCountry("GWN", "Gwynedd", country);
                StatePossessionAbbreviations.AddPairPlusCountry("GNT", "Gwent", country);
                StatePossessionAbbreviations.AddPairPlusCountry("GLA", "Glamorgan", country);
                StatePossessionAbbreviations.AddPairPlusCountry("FLN", "Flintshire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("CMN", "Carmarthenshire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("CGN", "Cardiganshire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("CWD", "Clwyd", country);
                StatePossessionAbbreviations.AddPairPlusCountry("DFD", "Dyfed", country);
                StatePossessionAbbreviations.AddPairPlusCountry("DEN", "Denbighshire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("BRE", "Breconshire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("CAE", "Caernarvonshire", country);
                StatePossessionAbbreviations.AddPairPlusCountry("AGY", "Anglesey", country);

                // Australia
                country = "Australia";
                StatePossessionAbbreviations.AddPairPlusCountry("Australian Capital Territory", "ACT", country);
                StatePossessionAbbreviations.AddPairPlusCountry("Jervis Bay Territory", "JBT", country);
                StatePossessionAbbreviations.AddPairPlusCountry("New South Wales", "NSW", country);
                StatePossessionAbbreviations.AddPairPlusCountry("Northern Territory", "NT", country);
                StatePossessionAbbreviations.AddPairPlusCountry("Queensland", "QLD", country);
                StatePossessionAbbreviations.AddPairPlusCountry("South Australia", "SA", country);
                StatePossessionAbbreviations.AddPairPlusCountry("Tasmania", "TAS", country);
                StatePossessionAbbreviations.AddPairPlusCountry("Victoria", "VIC", country);
                StatePossessionAbbreviations.AddPairPlusCountry("Western Australia", "WA", country);
            }
        }

        private void initGeographicDirectionalAbbreviations()
        {
            geographicDirectionalAbbreviations.AddPair("NORTH", "N");
            geographicDirectionalAbbreviations.AddPair("EAST", "E");
            geographicDirectionalAbbreviations.AddPair("SOUTH", "S");
            geographicDirectionalAbbreviations.AddPair("WEST", "W");
            geographicDirectionalAbbreviations.AddPair("SO", "S");
            geographicDirectionalAbbreviations.AddPair("NORTHEAST", "NE");
            geographicDirectionalAbbreviations.AddPair("SOUTHEAST", "SE");
            geographicDirectionalAbbreviations.AddPair("NORTHWEST", "NW");
            geographicDirectionalAbbreviations.AddPair("SOUTHWEST", "SW");
        }

        private void initSecondaryUnitDesignators()
        {
            secondaryUnitDesignators.AddPair("Apartment", "APT");
            secondaryUnitDesignators.AddPair("Basement", "BSMT");
            secondaryUnitDesignators.AddPair("Building", "BLDG");
            secondaryUnitDesignators.AddPair("Department", "DEPT");
            secondaryUnitDesignators.AddPair("Floor", "FL");
            secondaryUnitDesignators.AddPair("Front", "FRNT");
            secondaryUnitDesignators.AddPair("Hanger", "HNGR");
            secondaryUnitDesignators.AddPair("Key", "KEY");
            secondaryUnitDesignators.AddPair("Lobby", "LBBY");
            secondaryUnitDesignators.AddPair("Lot", "LOT");
            secondaryUnitDesignators.AddPair("Lower", "LOWR");
            secondaryUnitDesignators.AddPair("Office", "OFC");
            secondaryUnitDesignators.AddPair("Penthouse", "PH");
            secondaryUnitDesignators.AddPair("Pier", "PIER");
            secondaryUnitDesignators.AddPair("Rear", "REAR");
            secondaryUnitDesignators.AddPair("Room", "RM");
            secondaryUnitDesignators.AddPair("Side", "SIDE");
            secondaryUnitDesignators.AddPair("Slip", "SLIP");
            secondaryUnitDesignators.AddPair("Space", "SPC");
            secondaryUnitDesignators.AddPair("Stop", "STOP");
            secondaryUnitDesignators.AddPair("Suite", "STE");
            secondaryUnitDesignators.AddPair("Trailer", "TRLR");
            secondaryUnitDesignators.AddPair("Unit", "UNIT");
            secondaryUnitDesignators.AddPair("Upper", "UPPR");
            secondaryUnitDesignators.AddPair("#", "#");
        }

        private void initUspsAbbreviations()
        {
            streetSuffixAbbreviations.AddPair("ALLEE", "ALY");
            streetSuffixAbbreviations.AddPair("ALLEY", "ALY");
            streetSuffixAbbreviations.AddPair("ALLY", "ALY");
            streetSuffixAbbreviations.AddPair("ALY", "ALY");
            streetSuffixAbbreviations.AddPair("ANEX", "ANX");
            streetSuffixAbbreviations.AddPair("ANNEX", "ANX");
            streetSuffixAbbreviations.AddPair("ANNX", "ANX");
            streetSuffixAbbreviations.AddPair("ANX", "ANX");
            streetSuffixAbbreviations.AddPair("ARC", "ARC");
            streetSuffixAbbreviations.AddPair("ARCADE", "ARC");
            streetSuffixAbbreviations.AddPair("AV", "AVE");
            streetSuffixAbbreviations.AddPair("AVE", "AVE");
            streetSuffixAbbreviations.AddPair("AVEN", "AVE");
            streetSuffixAbbreviations.AddPair("AVENU", "AVE");
            streetSuffixAbbreviations.AddPair("AVENUE", "AVE");
            streetSuffixAbbreviations.AddPair("AVN", "AVE");
            streetSuffixAbbreviations.AddPair("AVNUE", "AVE");
            streetSuffixAbbreviations.AddPair("BAYOO", "BYU");
            streetSuffixAbbreviations.AddPair("BAYOU", "BYU");
            streetSuffixAbbreviations.AddPair("BCH", "BCH");
            streetSuffixAbbreviations.AddPair("BEACH", "BCH");
            streetSuffixAbbreviations.AddPair("BEND", "BND");
            streetSuffixAbbreviations.AddPair("BND", "BND");
            streetSuffixAbbreviations.AddPair("BLF", "BLF");
            streetSuffixAbbreviations.AddPair("BLUF", "BLF");
            streetSuffixAbbreviations.AddPair("BLUFF", "BLF");
            streetSuffixAbbreviations.AddPair("BLUFFS", "BLFS");
            streetSuffixAbbreviations.AddPair("BOT", "BTM");
            streetSuffixAbbreviations.AddPair("BOTTM", "BTM");
            streetSuffixAbbreviations.AddPair("BOTTOM", "BTM");
            streetSuffixAbbreviations.AddPair("BTM", "BTM");
            streetSuffixAbbreviations.AddPair("BLVD", "BLVD");
            streetSuffixAbbreviations.AddPair("BOUL", "BLVD");
            streetSuffixAbbreviations.AddPair("BOULEVARD", "BLVD");
            streetSuffixAbbreviations.AddPair("BOULV", "BLVD");
            streetSuffixAbbreviations.AddPair("BL", "BLVD"); // e-dule added
            streetSuffixAbbreviations.AddPair("BR", "BR");
            streetSuffixAbbreviations.AddPair("BRANCH", "BR");
            streetSuffixAbbreviations.AddPair("BRNCH", "BR");
            streetSuffixAbbreviations.AddPair("BRDGE", "BRG");
            streetSuffixAbbreviations.AddPair("BRG", "BRG");
            streetSuffixAbbreviations.AddPair("BRIDGE", "BRG");
            streetSuffixAbbreviations.AddPair("BRK", "BRK");
            streetSuffixAbbreviations.AddPair("BROOK", "BRK");
            streetSuffixAbbreviations.AddPair("BROOKS", "BRKS");
            streetSuffixAbbreviations.AddPair("BURG", "BG");
            streetSuffixAbbreviations.AddPair("BURGS", "BGS");
            streetSuffixAbbreviations.AddPair("BYP", "BYP");
            streetSuffixAbbreviations.AddPair("BYPA", "BYP");
            streetSuffixAbbreviations.AddPair("BYPAS", "BYP");
            streetSuffixAbbreviations.AddPair("BYPASS", "BYP");
            streetSuffixAbbreviations.AddPair("BYPS", "BYP");
            streetSuffixAbbreviations.AddPair("CAMP", "CP");
            streetSuffixAbbreviations.AddPair("CMP", "CP");
            streetSuffixAbbreviations.AddPair("CP", "CP");
            streetSuffixAbbreviations.AddPair("CANYN", "CYN");
            streetSuffixAbbreviations.AddPair("CANYON", "CYN");
            streetSuffixAbbreviations.AddPair("CNYN", "CYN");
            streetSuffixAbbreviations.AddPair("CYN", "CYN");
            streetSuffixAbbreviations.AddPair("CAPE", "CPE");
            streetSuffixAbbreviations.AddPair("CPE", "CPE");
            streetSuffixAbbreviations.AddPair("CAUSEWAY", "CSWY");
            streetSuffixAbbreviations.AddPair("CAUSWAY", "CSWY");
            streetSuffixAbbreviations.AddPair("CSWY", "CSWY");
            streetSuffixAbbreviations.AddPair("CEN", "CTR");
            streetSuffixAbbreviations.AddPair("CENT", "CTR");
            streetSuffixAbbreviations.AddPair("CENTER", "CTR");
            streetSuffixAbbreviations.AddPair("CENTR", "CTR");
            streetSuffixAbbreviations.AddPair("CENTRE", "CTR");
            streetSuffixAbbreviations.AddPair("CNTER", "CTR");
            streetSuffixAbbreviations.AddPair("CNTR", "CTR");
            streetSuffixAbbreviations.AddPair("CTR", "CTR");
            streetSuffixAbbreviations.AddPair("CENTERS", "CTRS");
            streetSuffixAbbreviations.AddPair("CIR", "CIR");
            streetSuffixAbbreviations.AddPair("CIRC", "CIR");
            streetSuffixAbbreviations.AddPair("CIRCL", "CIR");
            streetSuffixAbbreviations.AddPair("CIRCLE", "CIR");
            streetSuffixAbbreviations.AddPair("CRCL", "CIR");
            streetSuffixAbbreviations.AddPair("CRCLE", "CIR");
            streetSuffixAbbreviations.AddPair("CIRCLES", "CIRS");
            streetSuffixAbbreviations.AddPair("CLF", "CLF");
            streetSuffixAbbreviations.AddPair("CLIFF", "CLF");
            streetSuffixAbbreviations.AddPair("CLFS", "CLFS");
            streetSuffixAbbreviations.AddPair("CLIFFS", "CLFS");
            streetSuffixAbbreviations.AddPair("CLB", "CLB");
            streetSuffixAbbreviations.AddPair("CLUB", "CLB");
            streetSuffixAbbreviations.AddPair("COMMON", "CMN");
            streetSuffixAbbreviations.AddPair("COR", "COR");
            streetSuffixAbbreviations.AddPair("CORNER", "COR");
            streetSuffixAbbreviations.AddPair("CORNERS", "CORS");
            streetSuffixAbbreviations.AddPair("CORS", "CORS");
            streetSuffixAbbreviations.AddPair("COURSE", "CRSE");
            streetSuffixAbbreviations.AddPair("CRSE", "CRSE");
            streetSuffixAbbreviations.AddPair("COURT", "CT");
            streetSuffixAbbreviations.AddPair("CRT", "CT");
            streetSuffixAbbreviations.AddPair("CT", "CT");
            streetSuffixAbbreviations.AddPair("COURTS", "CTS");
            streetSuffixAbbreviations.AddPair("CTS", "CTS");
            streetSuffixAbbreviations.AddPair("COVE", "CV");
            streetSuffixAbbreviations.AddPair("CV", "CV");
            streetSuffixAbbreviations.AddPair("COVES", "CVS");
            streetSuffixAbbreviations.AddPair("CK", "CRK");
            streetSuffixAbbreviations.AddPair("CR", "CRK");
            streetSuffixAbbreviations.AddPair("CREEK", "CRK");
            streetSuffixAbbreviations.AddPair("CRK", "CRK");
            streetSuffixAbbreviations.AddPair("CRECENT", "CRES");
            streetSuffixAbbreviations.AddPair("CRES", "CRES");
            streetSuffixAbbreviations.AddPair("CRESCENT", "CRES");
            streetSuffixAbbreviations.AddPair("CRESENT", "CRES");
            streetSuffixAbbreviations.AddPair("CRSCNT", "CRES");
            streetSuffixAbbreviations.AddPair("CRSENT", "CRES");
            streetSuffixAbbreviations.AddPair("CRSNT", "CRES");
            streetSuffixAbbreviations.AddPair("CREST", "CRST");
            streetSuffixAbbreviations.AddPair("CROSSING", "XING");
            streetSuffixAbbreviations.AddPair("CRSSING", "XING");
            streetSuffixAbbreviations.AddPair("CRSSNG", "XING");
            streetSuffixAbbreviations.AddPair("XING", "XING");
            streetSuffixAbbreviations.AddPair("CROSSROAD", "XRD");
            streetSuffixAbbreviations.AddPair("CURVE", "CURV");
            streetSuffixAbbreviations.AddPair("DALE", "DL");
            streetSuffixAbbreviations.AddPair("DL", "DL");
            streetSuffixAbbreviations.AddPair("DAM", "DM");
            streetSuffixAbbreviations.AddPair("DM", "DM");
            streetSuffixAbbreviations.AddPair("DIV", "DV");
            streetSuffixAbbreviations.AddPair("DIVIDE", "DV");
            streetSuffixAbbreviations.AddPair("DV", "DV");
            streetSuffixAbbreviations.AddPair("DVD", "DV");
            streetSuffixAbbreviations.AddPair("DR", "DR");
            streetSuffixAbbreviations.AddPair("DRIV", "DR");
            streetSuffixAbbreviations.AddPair("DRIVE", "DR");
            streetSuffixAbbreviations.AddPair("DRV", "DR");
            streetSuffixAbbreviations.AddPair("DRIVES", "DRS");
            streetSuffixAbbreviations.AddPair("EST", "EST");
            streetSuffixAbbreviations.AddPair("ESTATE", "EST");
            streetSuffixAbbreviations.AddPair("ESTATES", "ESTS");
            streetSuffixAbbreviations.AddPair("ESTS", "ESTS");
            streetSuffixAbbreviations.AddPair("EXP", "EXPY");
            streetSuffixAbbreviations.AddPair("EXPR", "EXPY");
            streetSuffixAbbreviations.AddPair("EXPRESS", "EXPY");
            streetSuffixAbbreviations.AddPair("EXPRESSWAY", "EXPY");
            streetSuffixAbbreviations.AddPair("EXPW", "EXPY");
            streetSuffixAbbreviations.AddPair("EXPY", "EXPY");
            streetSuffixAbbreviations.AddPair("EXT", "EXT");
            streetSuffixAbbreviations.AddPair("EXTENSION", "EXT");
            streetSuffixAbbreviations.AddPair("EXTN", "EXT");
            streetSuffixAbbreviations.AddPair("EXTNSN", "EXT");
            streetSuffixAbbreviations.AddPair("EXTENSIONS", "EXTS");
            streetSuffixAbbreviations.AddPair("EXTS", "EXTS");
            streetSuffixAbbreviations.AddPair("FALL", "FALL");
            streetSuffixAbbreviations.AddPair("FALLS", "FLS");
            streetSuffixAbbreviations.AddPair("FLS", "FLS");
            streetSuffixAbbreviations.AddPair("FERRY", "FRY");
            streetSuffixAbbreviations.AddPair("FRRY", "FRY");
            streetSuffixAbbreviations.AddPair("FRY", "FRY");
            streetSuffixAbbreviations.AddPair("FIELD", "FLD");
            streetSuffixAbbreviations.AddPair("FLD", "FLD");
            streetSuffixAbbreviations.AddPair("FIELDS", "FLDS");
            streetSuffixAbbreviations.AddPair("FLDS", "FLDS");
            streetSuffixAbbreviations.AddPair("FLAT", "FLT");
            streetSuffixAbbreviations.AddPair("FLT", "FLT");
            streetSuffixAbbreviations.AddPair("FLATS", "FLTS");
            streetSuffixAbbreviations.AddPair("FLTS", "FLTS");
            streetSuffixAbbreviations.AddPair("FORD", "FRD");
            streetSuffixAbbreviations.AddPair("FRD", "FRD");
            streetSuffixAbbreviations.AddPair("FORDS", "FRDS");
            streetSuffixAbbreviations.AddPair("FOREST", "FRST");
            streetSuffixAbbreviations.AddPair("FORESTS", "FRST");
            streetSuffixAbbreviations.AddPair("FRST", "FRST");
            streetSuffixAbbreviations.AddPair("FORG", "FRG");
            streetSuffixAbbreviations.AddPair("FORGE", "FRG");
            streetSuffixAbbreviations.AddPair("FRG", "FRG");
            streetSuffixAbbreviations.AddPair("FORGES", "FRGS");
            streetSuffixAbbreviations.AddPair("FORK", "FRK");
            streetSuffixAbbreviations.AddPair("FRK", "FRK");
            streetSuffixAbbreviations.AddPair("FORKS", "FRKS");
            streetSuffixAbbreviations.AddPair("FRKS", "FRKS");
            streetSuffixAbbreviations.AddPair("FORT", "FT");
            streetSuffixAbbreviations.AddPair("FRT", "FT");
            streetSuffixAbbreviations.AddPair("FT", "FT");
            streetSuffixAbbreviations.AddPair("FREEWAY", "FWY");
            streetSuffixAbbreviations.AddPair("FREEWY", "FWY");
            streetSuffixAbbreviations.AddPair("FRWAY", "FWY");
            streetSuffixAbbreviations.AddPair("FRWY", "FWY");
            streetSuffixAbbreviations.AddPair("FWY", "FWY");
            streetSuffixAbbreviations.AddPair("GARDEN", "GDN");
            streetSuffixAbbreviations.AddPair("GARDN", "GDN");
            streetSuffixAbbreviations.AddPair("GDN", "GDN");
            streetSuffixAbbreviations.AddPair("GRDEN", "GDN");
            streetSuffixAbbreviations.AddPair("GRDN", "GDN");
            streetSuffixAbbreviations.AddPair("GARDENS", "GDNS");
            streetSuffixAbbreviations.AddPair("GDNS", "GDNS");
            streetSuffixAbbreviations.AddPair("GRDNS", "GDNS");
            streetSuffixAbbreviations.AddPair("GATEWAY", "GTWY");
            streetSuffixAbbreviations.AddPair("GATEWY", "GTWY");
            streetSuffixAbbreviations.AddPair("GATWAY", "GTWY");
            streetSuffixAbbreviations.AddPair("GTWAY", "GTWY");
            streetSuffixAbbreviations.AddPair("GTWY", "GTWY");
            streetSuffixAbbreviations.AddPair("GLEN", "GLN");
            streetSuffixAbbreviations.AddPair("GLN", "GLN");
            streetSuffixAbbreviations.AddPair("GLENS", "GLNS");
            streetSuffixAbbreviations.AddPair("GREEN", "GRN");
            streetSuffixAbbreviations.AddPair("GRN", "GRN");
            streetSuffixAbbreviations.AddPair("GREENS", "GRNS");
            streetSuffixAbbreviations.AddPair("GROV", "GRV");
            streetSuffixAbbreviations.AddPair("GROVE", "GRV");
            streetSuffixAbbreviations.AddPair("GRV", "GRV");
            streetSuffixAbbreviations.AddPair("GROVES", "GRVS");
            streetSuffixAbbreviations.AddPair("HARB", "HBR");
            streetSuffixAbbreviations.AddPair("HARBOR", "HBR");
            streetSuffixAbbreviations.AddPair("HARBR", "HBR");
            streetSuffixAbbreviations.AddPair("HBR", "HBR");
            streetSuffixAbbreviations.AddPair("HRBOR", "HBR");
            streetSuffixAbbreviations.AddPair("HARBORS", "HBRS");
            streetSuffixAbbreviations.AddPair("HAVEN", "HVN");
            streetSuffixAbbreviations.AddPair("HAVN", "HVN");
            streetSuffixAbbreviations.AddPair("HVN", "HVN");
            streetSuffixAbbreviations.AddPair("HEIGHT", "HTS");
            streetSuffixAbbreviations.AddPair("HEIGHTS", "HTS");
            streetSuffixAbbreviations.AddPair("HGTS", "HTS");
            streetSuffixAbbreviations.AddPair("HT", "HTS");
            streetSuffixAbbreviations.AddPair("HTS", "HTS");

            streetSuffixAbbreviations.AddPair("HIGHWAY", "HWY");
            streetSuffixAbbreviations.AddPair("HIGHWY", "HWY");
            streetSuffixAbbreviations.AddPair("HIWAY", "HWY");
            streetSuffixAbbreviations.AddPair("HIWY", "HWY");
            streetSuffixAbbreviations.AddPair("HWAY", "HWY");
            streetSuffixAbbreviations.AddPair("HWY", "HWY");

            streetSuffixAbbreviations.AddPair("US HIGHWAY", "HWY");
            streetSuffixAbbreviations.AddPair("US HIGHWY", "HWY");
            streetSuffixAbbreviations.AddPair("US HIWAY", "HWY");
            streetSuffixAbbreviations.AddPair("US HIWY", "HWY");
            streetSuffixAbbreviations.AddPair("US HWAY", "HWY");
            streetSuffixAbbreviations.AddPair("US HWY", "HWY");

            streetSuffixAbbreviations.AddPair("HILL", "HL");
            streetSuffixAbbreviations.AddPair("HL", "HL");
            streetSuffixAbbreviations.AddPair("HILLS", "HLS");
            streetSuffixAbbreviations.AddPair("HLS", "HLS");
            streetSuffixAbbreviations.AddPair("HLLW", "HOLW");
            streetSuffixAbbreviations.AddPair("HOLLOW", "HOLW");
            streetSuffixAbbreviations.AddPair("HOLLOWS", "HOLW");
            streetSuffixAbbreviations.AddPair("HOLW", "HOLW");
            streetSuffixAbbreviations.AddPair("HOLWS", "HOLW");
            streetSuffixAbbreviations.AddPair("INLET", "INLT");
            streetSuffixAbbreviations.AddPair("INLT", "INLT");
            streetSuffixAbbreviations.AddPair("IS", "IS");
            streetSuffixAbbreviations.AddPair("ISLAND", "IS");
            streetSuffixAbbreviations.AddPair("ISLND", "IS");
            streetSuffixAbbreviations.AddPair("ISLANDS", "ISS");
            streetSuffixAbbreviations.AddPair("ISLNDS", "ISS");
            streetSuffixAbbreviations.AddPair("ISS", "ISS");
            streetSuffixAbbreviations.AddPair("ISLE", "ISLE");
            streetSuffixAbbreviations.AddPair("ISLES", "ISLE");
            streetSuffixAbbreviations.AddPair("JCT", "JCT");
            streetSuffixAbbreviations.AddPair("JCTION", "JCT");
            streetSuffixAbbreviations.AddPair("JCTN", "JCT");
            streetSuffixAbbreviations.AddPair("JUNCTION", "JCT");
            streetSuffixAbbreviations.AddPair("JUNCTN", "JCT");
            streetSuffixAbbreviations.AddPair("JUNCTON", "JCT");
            streetSuffixAbbreviations.AddPair("JCTNS", "JCTS");
            streetSuffixAbbreviations.AddPair("JCTS", "JCTS");
            streetSuffixAbbreviations.AddPair("JUNCTIONS", "JCTS");
            streetSuffixAbbreviations.AddPair("KEY", "KY");
            streetSuffixAbbreviations.AddPair("KY", "KY");
            streetSuffixAbbreviations.AddPair("KEYS", "KYS");
            streetSuffixAbbreviations.AddPair("KYS", "KYS");
            streetSuffixAbbreviations.AddPair("KNL", "KNL");
            streetSuffixAbbreviations.AddPair("KNOL", "KNL");
            streetSuffixAbbreviations.AddPair("KNOLL", "KNL");
            streetSuffixAbbreviations.AddPair("KNLS", "KNLS");
            streetSuffixAbbreviations.AddPair("KNOLLS", "KNLS");
            streetSuffixAbbreviations.AddPair("LAKE", "LK");
            streetSuffixAbbreviations.AddPair("LK", "LK");
            streetSuffixAbbreviations.AddPair("LAKES", "LKS");
            streetSuffixAbbreviations.AddPair("LKS", "LKS");
            streetSuffixAbbreviations.AddPair("LAND", "LAND");
            streetSuffixAbbreviations.AddPair("LANDING", "LNDG");
            streetSuffixAbbreviations.AddPair("LNDG", "LNDG");
            streetSuffixAbbreviations.AddPair("LNDNG", "LNDG");
            streetSuffixAbbreviations.AddPair("LA", "LN");
            streetSuffixAbbreviations.AddPair("LANE", "LN");
            streetSuffixAbbreviations.AddPair("LANES", "LN");
            streetSuffixAbbreviations.AddPair("LN", "LN");
            streetSuffixAbbreviations.AddPair("LGT", "LGT");
            streetSuffixAbbreviations.AddPair("LIGHT", "LGT");
            streetSuffixAbbreviations.AddPair("LIGHTS", "LGTS");
            streetSuffixAbbreviations.AddPair("LF", "LF");
            streetSuffixAbbreviations.AddPair("LOAF", "LF");
            streetSuffixAbbreviations.AddPair("LCK", "LCK");
            streetSuffixAbbreviations.AddPair("LOCK", "LCK");
            streetSuffixAbbreviations.AddPair("LCKS", "LCKS");
            streetSuffixAbbreviations.AddPair("LOCKS", "LCKS");
            streetSuffixAbbreviations.AddPair("LDG", "LDG");
            streetSuffixAbbreviations.AddPair("LDGE", "LDG");
            streetSuffixAbbreviations.AddPair("LODG", "LDG");
            streetSuffixAbbreviations.AddPair("LODGE", "LDG");
            streetSuffixAbbreviations.AddPair("LP", "LOOP");
            streetSuffixAbbreviations.AddPair("LOOP", "LOOP");
            streetSuffixAbbreviations.AddPair("LOOPS", "LOOP");
            streetSuffixAbbreviations.AddPair("MALL", "MALL");
            streetSuffixAbbreviations.AddPair("MANOR", "MNR");
            streetSuffixAbbreviations.AddPair("MNR", "MNR");
            streetSuffixAbbreviations.AddPair("MANORS", "MNRS");
            streetSuffixAbbreviations.AddPair("MNRS", "MNRS");
            streetSuffixAbbreviations.AddPair("MDW", "MDW");
            streetSuffixAbbreviations.AddPair("MEADOW", "MDW");
            streetSuffixAbbreviations.AddPair("MDWS", "MDWS");
            streetSuffixAbbreviations.AddPair("MEADOWS", "MDWS");
            streetSuffixAbbreviations.AddPair("MEDOWS", "MDWS");
            streetSuffixAbbreviations.AddPair("MEWS", "MEWS");
            streetSuffixAbbreviations.AddPair("MILL", "ML");
            streetSuffixAbbreviations.AddPair("ML", "ML");
            streetSuffixAbbreviations.AddPair("MILLS", "MLS");
            streetSuffixAbbreviations.AddPair("MLS", "MLS");
            streetSuffixAbbreviations.AddPair("MISSION", "MSN");
            streetSuffixAbbreviations.AddPair("MISSN", "MSN");
            streetSuffixAbbreviations.AddPair("MSN", "MSN");
            streetSuffixAbbreviations.AddPair("MSSN", "MSN");
            streetSuffixAbbreviations.AddPair("MOTORWAY", "MTWY");
            streetSuffixAbbreviations.AddPair("MNT", "MT");
            streetSuffixAbbreviations.AddPair("MOUNT", "MT");
            streetSuffixAbbreviations.AddPair("MT", "MT");
            streetSuffixAbbreviations.AddPair("MNTAIN", "MTN");
            streetSuffixAbbreviations.AddPair("MNTN", "MTN");
            streetSuffixAbbreviations.AddPair("MOUNTAIN", "MTN");
            streetSuffixAbbreviations.AddPair("MOUNTIN", "MTN");
            streetSuffixAbbreviations.AddPair("MTIN", "MTN");
            streetSuffixAbbreviations.AddPair("MTN", "MTN");
            streetSuffixAbbreviations.AddPair("MNTNS", "MTNS");
            streetSuffixAbbreviations.AddPair("MOUNTAINS", "MTNS");
            streetSuffixAbbreviations.AddPair("NCK", "NCK");
            streetSuffixAbbreviations.AddPair("NECK", "NCK");
            streetSuffixAbbreviations.AddPair("ORCH", "ORCH");
            streetSuffixAbbreviations.AddPair("ORCHARD", "ORCH");
            streetSuffixAbbreviations.AddPair("ORCHRD", "ORCH");
            streetSuffixAbbreviations.AddPair("OVAL", "OVAL");
            streetSuffixAbbreviations.AddPair("OVL", "OVAL");
            streetSuffixAbbreviations.AddPair("OVERPASS", "OPAS");
            streetSuffixAbbreviations.AddPair("PARK", "PARK");
            streetSuffixAbbreviations.AddPair("PK", "PARK");
            streetSuffixAbbreviations.AddPair("PRK", "PARK");
            streetSuffixAbbreviations.AddPair("PARKS", "PARK");
            streetSuffixAbbreviations.AddPair("PARKWAY", "PKWY");
            streetSuffixAbbreviations.AddPair("PARKWY", "PKWY");
            streetSuffixAbbreviations.AddPair("PKWAY", "PKWY");
            streetSuffixAbbreviations.AddPair("PKWY", "PKWY");
            streetSuffixAbbreviations.AddPair("PKY", "PKWY");
            streetSuffixAbbreviations.AddPair("PARKWAYS", "PKWY");
            streetSuffixAbbreviations.AddPair("PKWYS", "PKWY");
            streetSuffixAbbreviations.AddPair("PASS", "PASS");
            streetSuffixAbbreviations.AddPair("PASSAGE", "PSGE");
            streetSuffixAbbreviations.AddPair("PATH", "PATH");
            streetSuffixAbbreviations.AddPair("PATHS", "PATH");
            streetSuffixAbbreviations.AddPair("PIKE", "PIKE");
            streetSuffixAbbreviations.AddPair("PIKES", "PIKE");
            streetSuffixAbbreviations.AddPair("PINE", "PNE");
            streetSuffixAbbreviations.AddPair("PINES", "PNES");
            streetSuffixAbbreviations.AddPair("PNES", "PNES");
            streetSuffixAbbreviations.AddPair("PL", "PL");
            streetSuffixAbbreviations.AddPair("PLACE", "PL");
            streetSuffixAbbreviations.AddPair("PLAIN", "PLN");
            streetSuffixAbbreviations.AddPair("PLN", "PLN");
            streetSuffixAbbreviations.AddPair("PLAINES", "PLNS");
            streetSuffixAbbreviations.AddPair("PLAINS", "PLNS");
            streetSuffixAbbreviations.AddPair("PLNS", "PLNS");
            streetSuffixAbbreviations.AddPair("PLAZA", "PLZ");
            streetSuffixAbbreviations.AddPair("PLZ", "PLZ");
            streetSuffixAbbreviations.AddPair("PLZA", "PLZ");
            streetSuffixAbbreviations.AddPair("POINT", "PT");
            streetSuffixAbbreviations.AddPair("PT", "PT");
            streetSuffixAbbreviations.AddPair("POINTS", "PTS");
            streetSuffixAbbreviations.AddPair("PTS", "PTS");
            streetSuffixAbbreviations.AddPair("PORT", "PRT");
            streetSuffixAbbreviations.AddPair("PRT", "PRT");
            streetSuffixAbbreviations.AddPair("PORTS", "PRTS");
            streetSuffixAbbreviations.AddPair("PRTS", "PRTS");
            streetSuffixAbbreviations.AddPair("PR", "PR");
            streetSuffixAbbreviations.AddPair("PRAIRIE", "PR");
            streetSuffixAbbreviations.AddPair("PRARIE", "PR");
            streetSuffixAbbreviations.AddPair("PRR", "PR");
            streetSuffixAbbreviations.AddPair("RAD", "RADL");
            streetSuffixAbbreviations.AddPair("RADIAL", "RADL");
            streetSuffixAbbreviations.AddPair("RADIEL", "RADL");
            streetSuffixAbbreviations.AddPair("RADL", "RADL");
            streetSuffixAbbreviations.AddPair("RAMP", "RAMP");
            streetSuffixAbbreviations.AddPair("RANCH", "RNCH");
            streetSuffixAbbreviations.AddPair("RANCHES", "RNCH");
            streetSuffixAbbreviations.AddPair("RNCH", "RNCH");
            streetSuffixAbbreviations.AddPair("RNCHS", "RNCH");
            streetSuffixAbbreviations.AddPair("RAPID", "RPD");
            streetSuffixAbbreviations.AddPair("RPD", "RPD");
            streetSuffixAbbreviations.AddPair("RAPIDS", "RPDS");
            streetSuffixAbbreviations.AddPair("RPDS", "RPDS");
            streetSuffixAbbreviations.AddPair("REST", "RST");
            streetSuffixAbbreviations.AddPair("RST", "RST");
            streetSuffixAbbreviations.AddPair("RDG", "RDG");
            streetSuffixAbbreviations.AddPair("RDGE", "RDG");
            streetSuffixAbbreviations.AddPair("RIDGE", "RDG");
            streetSuffixAbbreviations.AddPair("RDGS", "RDGS");
            streetSuffixAbbreviations.AddPair("RIDGES", "RDGS");
            streetSuffixAbbreviations.AddPair("RIV", "RIV");
            streetSuffixAbbreviations.AddPair("RIVER", "RIV");
            streetSuffixAbbreviations.AddPair("RIVR", "RIV");
            streetSuffixAbbreviations.AddPair("RVR", "RIV");
            streetSuffixAbbreviations.AddPair("RD", "RD");
            streetSuffixAbbreviations.AddPair("ROAD", "RD");
            streetSuffixAbbreviations.AddPair("RDS", "RDS");
            streetSuffixAbbreviations.AddPair("ROADS", "RDS");
            streetSuffixAbbreviations.AddPair("ROUTE", "RTE");
            streetSuffixAbbreviations.AddPair("ROW", "ROW");
            streetSuffixAbbreviations.AddPair("RUE", "RUE");
            streetSuffixAbbreviations.AddPair("RUN", "RUN");
            streetSuffixAbbreviations.AddPair("SHL", "SHL");
            streetSuffixAbbreviations.AddPair("SHOAL", "SHL");
            streetSuffixAbbreviations.AddPair("SHLS", "SHLS");
            streetSuffixAbbreviations.AddPair("SHOALS", "SHLS");
            streetSuffixAbbreviations.AddPair("SHOAR", "SHR");
            streetSuffixAbbreviations.AddPair("SHORE", "SHR");
            streetSuffixAbbreviations.AddPair("SHR", "SHR");
            streetSuffixAbbreviations.AddPair("SHOARS", "SHRS");
            streetSuffixAbbreviations.AddPair("SHORES", "SHRS");
            streetSuffixAbbreviations.AddPair("SHRS", "SHRS");
            streetSuffixAbbreviations.AddPair("SKYWAY", "SKWY");
            streetSuffixAbbreviations.AddPair("SPG", "SPG");
            streetSuffixAbbreviations.AddPair("SPNG", "SPG");
            streetSuffixAbbreviations.AddPair("SPRING", "SPG");
            streetSuffixAbbreviations.AddPair("SPRNG", "SPG");
            streetSuffixAbbreviations.AddPair("SPGS", "SPGS");
            streetSuffixAbbreviations.AddPair("SPNGS", "SPGS");
            streetSuffixAbbreviations.AddPair("SPRINGS", "SPGS");
            streetSuffixAbbreviations.AddPair("SPRNGS", "SPGS");
            streetSuffixAbbreviations.AddPair("SPUR", "SPUR");
            streetSuffixAbbreviations.AddPair("SPURS", "SPUR");
            streetSuffixAbbreviations.AddPair("SQ", "SQ");
            streetSuffixAbbreviations.AddPair("SQR", "SQ");
            streetSuffixAbbreviations.AddPair("SQRE", "SQ");
            streetSuffixAbbreviations.AddPair("SQU", "SQ");
            streetSuffixAbbreviations.AddPair("SQUARE", "SQ");
            streetSuffixAbbreviations.AddPair("SQRS", "SQS");
            streetSuffixAbbreviations.AddPair("SQUARES", "SQS");
            streetSuffixAbbreviations.AddPair("STA", "STA");
            streetSuffixAbbreviations.AddPair("STATION", "STA");
            streetSuffixAbbreviations.AddPair("STATN", "STA");
            streetSuffixAbbreviations.AddPair("STN", "STA");
            streetSuffixAbbreviations.AddPair("STRA", "STRA");
            streetSuffixAbbreviations.AddPair("STRAV", "STRA");
            streetSuffixAbbreviations.AddPair("STRAVE", "STRA");
            streetSuffixAbbreviations.AddPair("STRAVEN", "STRA");
            streetSuffixAbbreviations.AddPair("STRAVENUE", "STRA");
            streetSuffixAbbreviations.AddPair("STRAVN", "STRA");
            streetSuffixAbbreviations.AddPair("STRVN", "STRA");
            streetSuffixAbbreviations.AddPair("STRVNUE", "STRA");
            streetSuffixAbbreviations.AddPair("STREAM", "STRM");
            streetSuffixAbbreviations.AddPair("STREME", "STRM");
            streetSuffixAbbreviations.AddPair("STRM", "STRM");
            streetSuffixAbbreviations.AddPair("ST", "ST");
            streetSuffixAbbreviations.AddPair("STR", "ST");
            streetSuffixAbbreviations.AddPair("STREET", "ST");
            streetSuffixAbbreviations.AddPair("STRT", "ST");
            streetSuffixAbbreviations.AddPair("STREETS", "STS");
            streetSuffixAbbreviations.AddPair("SMT", "SMT");
            streetSuffixAbbreviations.AddPair("SUMIT", "SMT");
            streetSuffixAbbreviations.AddPair("SUMITT", "SMT");
            streetSuffixAbbreviations.AddPair("SUMMIT", "SMT");
            streetSuffixAbbreviations.AddPair("TER", "TER");
            streetSuffixAbbreviations.AddPair("TERR", "TER");
            streetSuffixAbbreviations.AddPair("TERRACE", "TER");
            streetSuffixAbbreviations.AddPair("THROUGHWAY", "TRWY");
            streetSuffixAbbreviations.AddPair("TRACE", "TRCE");
            streetSuffixAbbreviations.AddPair("TRACES", "TRCE");
            streetSuffixAbbreviations.AddPair("TRCE", "TRCE");
            streetSuffixAbbreviations.AddPair("TRACK", "TRAK");
            streetSuffixAbbreviations.AddPair("TRACKS", "TRAK");
            streetSuffixAbbreviations.AddPair("TRAK", "TRAK");
            streetSuffixAbbreviations.AddPair("TRK", "TRAK");
            streetSuffixAbbreviations.AddPair("TRKS", "TRAK");
            streetSuffixAbbreviations.AddPair("TRAFFICWAY", "TRFY");
            streetSuffixAbbreviations.AddPair("TRFY", "TRFY");
            streetSuffixAbbreviations.AddPair("TR", "TRL");
            streetSuffixAbbreviations.AddPair("TRAIL", "TRL");
            streetSuffixAbbreviations.AddPair("TRAILS", "TRL");
            streetSuffixAbbreviations.AddPair("TRL", "TRL");
            streetSuffixAbbreviations.AddPair("TRLS", "TRL");
            streetSuffixAbbreviations.AddPair("TUNEL", "TUNL");
            streetSuffixAbbreviations.AddPair("TUNL", "TUNL");
            streetSuffixAbbreviations.AddPair("TUNLS", "TUNL");
            streetSuffixAbbreviations.AddPair("TUNNEL", "TUNL");
            streetSuffixAbbreviations.AddPair("TUNNELS", "TUNL");
            streetSuffixAbbreviations.AddPair("TUNNL", "TUNL");
            streetSuffixAbbreviations.AddPair("TPK", "TPKE");
            streetSuffixAbbreviations.AddPair("TPKE", "TPKE");
            streetSuffixAbbreviations.AddPair("TRNPK", "TPKE");
            streetSuffixAbbreviations.AddPair("TRPK", "TPKE");
            streetSuffixAbbreviations.AddPair("TURNPIKE", "TPKE");
            streetSuffixAbbreviations.AddPair("TURNPK", "TPKE");
            streetSuffixAbbreviations.AddPair("UNDERPASS", "UPAS");
            streetSuffixAbbreviations.AddPair("UN", "UN");
            streetSuffixAbbreviations.AddPair("UNION", "UN");
            streetSuffixAbbreviations.AddPair("UNIONS", "UNS");
            streetSuffixAbbreviations.AddPair("VALLEY", "VLY");
            streetSuffixAbbreviations.AddPair("VALLY", "VLY");
            streetSuffixAbbreviations.AddPair("VLLY", "VLY");
            streetSuffixAbbreviations.AddPair("VLY", "VLY");
            streetSuffixAbbreviations.AddPair("VALLEYS", "VLYS");
            streetSuffixAbbreviations.AddPair("VLYS", "VLYS");
            streetSuffixAbbreviations.AddPair("VDCT", "VIA");
            streetSuffixAbbreviations.AddPair("VIA", "VIA");
            streetSuffixAbbreviations.AddPair("VIADCT", "VIA");
            streetSuffixAbbreviations.AddPair("VIADUCT", "VIA");
            streetSuffixAbbreviations.AddPair("VIEW", "VW");
            streetSuffixAbbreviations.AddPair("VW", "VW");
            streetSuffixAbbreviations.AddPair("VIEWS", "VWS");
            streetSuffixAbbreviations.AddPair("VWS", "VWS");
            streetSuffixAbbreviations.AddPair("VILL", "VLG");
            streetSuffixAbbreviations.AddPair("VILLAG", "VLG");
            streetSuffixAbbreviations.AddPair("VILLAGE", "VLG");
            streetSuffixAbbreviations.AddPair("VILLG", "VLG");
            streetSuffixAbbreviations.AddPair("VILLIAGE", "VLG");
            streetSuffixAbbreviations.AddPair("VLG", "VLG");
            streetSuffixAbbreviations.AddPair("VILLAGES", "VLGS");
            streetSuffixAbbreviations.AddPair("VLGS", "VLGS");
            streetSuffixAbbreviations.AddPair("VILLE", "VL");
            streetSuffixAbbreviations.AddPair("VL", "VL");
            streetSuffixAbbreviations.AddPair("VIS", "VIS");
            streetSuffixAbbreviations.AddPair("VIST", "VIS");
            streetSuffixAbbreviations.AddPair("VISTA", "VIS");
            streetSuffixAbbreviations.AddPair("VST", "VIS");
            streetSuffixAbbreviations.AddPair("VSTA", "VIS");
            streetSuffixAbbreviations.AddPair("WALK", "WALK");
            streetSuffixAbbreviations.AddPair("WALKS", "WALK");
            streetSuffixAbbreviations.AddPair("WALL", "WALL");
            streetSuffixAbbreviations.AddPair("WAY", "WAY");
            streetSuffixAbbreviations.AddPair("WY", "WAY");
            streetSuffixAbbreviations.AddPair("WAYS", "WAYS");
            streetSuffixAbbreviations.AddPair("WELL", "WL");
            streetSuffixAbbreviations.AddPair("WELLS", "WLS");
            streetSuffixAbbreviations.AddPair("WLS", "WLS");
        }

        private void add10RegularOrdinalWords(int start, string ordinal, string cardinal)
        {
            ordinalWords.AddPair(cardinal, start.ToString() + "TH");
            ordinalWords.AddPair(ordinal + "-FIRST", (start + 1).ToString() + "ST");
            ordinalWords.AddPair(ordinal + "-SECOND", (start + 2).ToString() + "ND");
            ordinalWords.AddPair(ordinal + "-THIRD", (start + 3).ToString() + "RD");
            ordinalWords.AddPair(ordinal + "-FOURTH", (start + 4).ToString() + "TH");
            ordinalWords.AddPair(ordinal + "-FIFTH", (start + 5).ToString() + "TH");
            ordinalWords.AddPair(ordinal + "-SIXTH", (start + 6).ToString() + "TH");
            ordinalWords.AddPair(ordinal + "-SEVENTH", (start + 7).ToString() + "TH");
            ordinalWords.AddPair(ordinal + "-EIGHTH", (start + 8).ToString() + "TH");
            ordinalWords.AddPair(ordinal + "-NINTH", (start + 9).ToString() + "TH");

            ordinalWords.AddPair(ordinal + " FIRST", (start + 1).ToString() + "ST");
            ordinalWords.AddPair(ordinal + " SECOND", (start + 2).ToString() + "ND");
            ordinalWords.AddPair(ordinal + " THIRD", (start + 3).ToString() + "RD");
            ordinalWords.AddPair(ordinal + " FOURTH", (start + 4).ToString() + "TH");
            ordinalWords.AddPair(ordinal + " FIFTH", (start + 5).ToString() + "TH");
            ordinalWords.AddPair(ordinal + " SIXTH", (start + 6).ToString() + "TH");
            ordinalWords.AddPair(ordinal + " SEVENTH", (start + 7).ToString() + "TH");
            ordinalWords.AddPair(ordinal + " EIGHTH", (start + 8).ToString() + "TH");
            ordinalWords.AddPair(ordinal + " NINTH", (start + 9).ToString() + "TH");
        }

        private void initOrdinalWords()
        {
            ordinalWords.AddPair("FIRST", "1ST");
            ordinalWords.AddPair("SECOND", "2ND");
            ordinalWords.AddPair("THIRD", "3RD");
            ordinalWords.AddPair("FOURTH", "4TH");
            ordinalWords.AddPair("FIFTH", "5TH");
            ordinalWords.AddPair("SIXTH", "6TH");
            ordinalWords.AddPair("SEVENTH", "7TH");
            ordinalWords.AddPair("EIGHTH", "8TH");
            ordinalWords.AddPair("NINETH", "9TH");
            ordinalWords.AddPair("TENTH", "10TH");

            ordinalWords.AddPair("ELEVENTH", "11TH");
            ordinalWords.AddPair("TWELFTH", "12TH");
            ordinalWords.AddPair("THIRTEENTH", "13TH");
            ordinalWords.AddPair("FOURTEETH", "14TH");
            ordinalWords.AddPair("FIFTEENTH", "15TH");
            ordinalWords.AddPair("SIXTEENTH", "16TH");
            ordinalWords.AddPair("SEVENTEENTH", "17TH");
            ordinalWords.AddPair("EIGHTEENTH", "18TH");
            ordinalWords.AddPair("NINETEENTH ", "19TH");

            add10RegularOrdinalWords(20, "TWENTY", "TWENTIETH");
            add10RegularOrdinalWords(30, "THIRTY", "THIRTIETH");
            add10RegularOrdinalWords(40, "FORTY", "FORTIETH");
            add10RegularOrdinalWords(50, "FIFTY", "FIFTIETH");
            add10RegularOrdinalWords(60, "SIXTY", "SIXTIETH");
            add10RegularOrdinalWords(70, "SEVENTY", "SEVENTIETH");
            add10RegularOrdinalWords(80, "EIGHTY", "EIGHTIETH");
            add10RegularOrdinalWords(90, "NINETY", "NINETIETH");
        }

        private void initNumberWords()
        {
            bool toAddKeyOnly = false;
            numberWords.AddPair("one", "1", toAddKeyOnly);
            numberWords.AddPair("two", "2", toAddKeyOnly);
            numberWords.AddPair("three", "3", toAddKeyOnly);
            numberWords.AddPair("four", "4", toAddKeyOnly);
            numberWords.AddPair("five", "5", toAddKeyOnly);
            numberWords.AddPair("six", "6", toAddKeyOnly);
            numberWords.AddPair("seven", "7", toAddKeyOnly);
            numberWords.AddPair("eight", "8", toAddKeyOnly);
            numberWords.AddPair("nine", "9", toAddKeyOnly);
            numberWords.AddPair("ten", "10", toAddKeyOnly);
        }

        private void initPostalBoxWords()
        {
            postalBoxPrefixWords.Add("PO BOX");
            postalBoxPrefixWords.Add("POBOX");
            postalBoxPrefixWords.Add("POST OFFICE BOX");
            postalBoxPrefixWords.Add("P.O.BOX");
            postalBoxPrefixWords.Add("P.O. BOX");
            postalBoxPrefixWords.Add("CALLER");
            postalBoxPrefixWords.Add("FIRM CALLER");
            postalBoxPrefixWords.Add("BIN");
            postalBoxPrefixWords.Add("LOCKBOX");
            postalBoxPrefixWords.Add("PO DRAWER");
            postalBoxPrefixWords.Add("PODRAWER");
            postalBoxPrefixWords.Add("P.O. DRAWER");
            postalBoxPrefixWords.Add("P.O.DRAWER");
            postalBoxPrefixWords.Add("POST OFFICE DRAWER");
            postalBoxPrefixWords.Add("DRAWER");
        }

        private void initRuralRouteWords()
        {
            ruralRoutePrefixWords.Add("RR");
            ruralRoutePrefixWords.Add("HC");
        }

        private void initBoxWords()
        {
            boxWords.Add("BOX");
        }

        public string GetStateAbbrevation(string state)
        {
            if (StatePossessionAbbreviations.Count == 0)
            {
                initStatePossessionAbbreviations();
            }

            string result = StatePossessionAbbreviations.GetValue(state);
            return result;
        }

        #endregion

        #region overridden methods

        protected override void initPatterns()
        {
            countriesPatternsGroup.Add(new Pattern(new List<int> { (int)RecognizedTypes.Country },
                countriesPatternsGroup, typeof(RecognizedTypes)));
            patternsGroupList.Add(countriesPatternsGroup);

            citiesPatternsGroup.Add(new Pattern(new List<int> { (int)RecognizedTypes.City }, citiesPatternsGroup,
                typeof(RecognizedTypes)));
            patternsGroupList.Add(citiesPatternsGroup);

            zipPatternsGroup.Add(new Pattern(new List<int> { (int)RecognizedTypes.Numeric5Digits }, zipPatternsGroup,
                typeof(RecognizedTypes)));
            patternsGroupList.Add(zipPatternsGroup);

            canadianZipPatternsGroup.Add(new Pattern(
                new List<int>
                {
                    (int) RecognizedTypes.AlphaNumericAlpha,
                    (int) RecognizedTypes.NumericAlphaNumeric
                }, canadianZipPatternsGroup, typeof(RecognizedTypes)));
            canadianZipPatternsGroup.Add(new Pattern(new List<int> { (int)RecognizedTypes.CanadianZip },
                canadianZipPatternsGroup, typeof(RecognizedTypes)));
            patternsGroupList.Add(canadianZipPatternsGroup);

            usZip9PatternsGroup.Add(new Pattern(
                new List<int>
                {
                    (int) RecognizedTypes.Numeric5Digits,
                    (int) RecognizedTypes.Hyphen,
                    (int) RecognizedTypes.Numeric4Digits
                }, usZip9PatternsGroup, typeof(RecognizedTypes)));
            patternsGroupList.Add(usZip9PatternsGroup);

            geoDirectionPatternsGroup.Add(new Pattern(new List<int> { (int)RecognizedTypes.GeoDirection },
                geoDirectionPatternsGroup, typeof(RecognizedTypes)));
            patternsGroupList.Add(geoDirectionPatternsGroup);

            statePatternsGroup.Add(new Pattern(new List<int> { (int)RecognizedTypes.State }, statePatternsGroup,
                typeof(RecognizedTypes)));
            patternsGroupList.Add(statePatternsGroup);

            postalBoxPatternsGroup.Add(new Pattern(
                new List<int> { (int)RecognizedTypes.PoBoxPrefix, (int)RecognizedTypes.Numeric },
                postalBoxPatternsGroup, typeof(RecognizedTypes)));
            postalBoxPatternsGroup.Add(new Pattern(
                new List<int> { (int)RecognizedTypes.PoBoxPrefix, (int)RecognizedTypes.AlphaNumeric },
                postalBoxPatternsGroup, typeof(RecognizedTypes)));
            postalBoxPatternsGroup.Add(new Pattern(
                new List<int>
                {
                    (int) RecognizedTypes.RuralRoutePrefix,
                    (int) RecognizedTypes.Numeric,
                    (int) RecognizedTypes.Box,
                    (int) RecognizedTypes.Numeric
                }, postalBoxPatternsGroup, typeof(RecognizedTypes)));
            postalBoxPatternsGroup.Add(new Pattern(
                new List<int>
                {
                    (int) RecognizedTypes.RuralRoutePrefix,
                    (int) RecognizedTypes.Numeric,
                    (int) RecognizedTypes.Box,
                    (int) RecognizedTypes.AlphaNumeric
                }, postalBoxPatternsGroup, typeof(RecognizedTypes)));
            patternsGroupList.Add(postalBoxPatternsGroup);

            boxPatternsGroup.Add(new Pattern(
                new List<int> { (int)RecognizedTypes.Box, (int)RecognizedTypes.Numeric }, boxPatternsGroup,
                typeof(RecognizedTypes)));
            boxPatternsGroup.Add(new Pattern(
                new List<int> { (int)RecognizedTypes.Box, (int)RecognizedTypes.AlphaNumeric }, boxPatternsGroup,
                typeof(RecognizedTypes)));
            patternsGroupList.Add(boxPatternsGroup);

            secondaryAddressUnitPatternsGroup.Add(new Pattern(
                new List<int> { (int)RecognizedTypes.SecondaryUnitDesignator, (int)RecognizedTypes.Numeric },
                secondaryAddressUnitPatternsGroup, typeof(RecognizedTypes)));
            secondaryAddressUnitPatternsGroup.Add(new Pattern(
                new List<int> { (int)RecognizedTypes.SecondaryUnitDesignator, (int)RecognizedTypes.AlphaNumeric },
                secondaryAddressUnitPatternsGroup, typeof(RecognizedTypes)));
            secondaryAddressUnitPatternsGroup.Add(new Pattern(
                new List<int>
                {
                    (int) RecognizedTypes.SecondaryUnitDesignator,
                    (int) RecognizedTypes.UndefinedLetters
                }, secondaryAddressUnitPatternsGroup, typeof(RecognizedTypes)));
            patternsGroupList.Add(secondaryAddressUnitPatternsGroup);

            streetSuffixPatternsGroup.Add(new Pattern(new List<int> { (int)RecognizedTypes.StreetSuffix },
                streetSuffixPatternsGroup, typeof(RecognizedTypes)));
            patternsGroupList.Add(streetSuffixPatternsGroup);

            streetNumberPatternsGroup.Add(new Pattern(
                new List<int>
                {
                    (int) RecognizedTypes.UndefinedLetters,
                    (int) RecognizedTypes.Hyphen,
                    (int) RecognizedTypes.Numeric
                }, streetNumberPatternsGroup, typeof(RecognizedTypes)));
            streetNumberPatternsGroup.Add(new Pattern(
                new List<int>
                {
                    (int) RecognizedTypes.UndefinedLetters,
                    (int) RecognizedTypes.Hyphen,
                    (int) RecognizedTypes.AlphaNumeric
                }, streetNumberPatternsGroup, typeof(RecognizedTypes)));
            streetNumberPatternsGroup.Add(new Pattern(new List<int> { (int)RecognizedTypes.Numeric },
                streetNumberPatternsGroup, typeof(RecognizedTypes)));
            streetNumberPatternsGroup.Add(new Pattern(
                new List<int>
                {
                    (int) RecognizedTypes.Numeric,
                    (int) RecognizedTypes.Hyphen,
                    (int) RecognizedTypes.Numeric
                }, streetNumberPatternsGroup, typeof(RecognizedTypes)));
            streetNumberPatternsGroup.Add(new Pattern(new List<int> { (int)RecognizedTypes.AlphaNumeric },
                streetNumberPatternsGroup, typeof(RecognizedTypes)));
            streetNumberPatternsGroup.Add(new Pattern(
                new List<int>
                {
                    (int) RecognizedTypes.AlphaNumeric,
                    (int) RecognizedTypes.Hyphen,
                    (int) RecognizedTypes.AlphaNumeric
                }, streetNumberPatternsGroup, typeof(RecognizedTypes)));
            streetNumberPatternsGroup.Add(new Pattern(new List<int> { (int)RecognizedTypes.NumberWord },
                streetNumberPatternsGroup, typeof(RecognizedTypes)));
            patternsGroupList.Add(streetNumberPatternsGroup);
        }

        protected override Dictionary<int, bool> findCategories(string s)
        {
            Dictionary<int, bool> result = new Dictionary<int, bool>();
            if (countryAbbreviations.IsMember(s))
            {
                result.Add(countryAbbreviations.CategoryId, false);
            }

            if (StatePossessionAbbreviations.GetValue(s) != null)
            {
                result.Add(StatePossessionAbbreviations.CategoryId, false);
            }

            if (geographicDirectionalAbbreviations.IsMember(s)
            ) // without else because of NorthEast and NEBRASKA : NE and NE...
            {
                result.Add(geographicDirectionalAbbreviations.CategoryId, false);
            }
            else if (secondaryUnitDesignators.IsMember(s))
            {
                result.Add(secondaryUnitDesignators.CategoryId, false);
            }
            else if (streetSuffixAbbreviations.IsMember(s))
            {
                result.Add(streetSuffixAbbreviations.CategoryId, false);
            }
            else if (ordinalWords.IsMember(s))
            {
                result.Add(ordinalWords.CategoryId, false);
            }
            else if (numberWords.IsMember(s))
            {
                result.Add(numberWords.CategoryId, false);
            }
            else if (postalBoxPrefixWords.IsMember(s))
            {
                result.Add(postalBoxPrefixWords.CategoryId, false);
            }
            else if (ruralRoutePrefixWords.IsMember(s))
            {
                result.Add(ruralRoutePrefixWords.CategoryId, false);
            }
            else if (boxWords.IsMember(s))
            {
                result.Add(boxWords.CategoryId, false);
            }
            else if (s == "-")
            {
                result.Add((int)RecognizedTypes.Hyphen, false);
            }
            else if (isNumeric(s))
            {
                result.Add((int)RecognizedTypes.Numeric, false);
                if (s.Length == 4)
                {
                    result.Add((int)RecognizedTypes.Numeric4Digits, false);
                }
                else if (s.Length == 5)
                {
                    result.Add((int)RecognizedTypes.Numeric5Digits, false);
                }
            }
            else if (isAlphaNumeric(s))
            {
                result.Add((int)RecognizedTypes.AlphaNumeric, false);
                if (isCanadianZip(s))
                {
                    result.Add((int)RecognizedTypes.CanadianZip, false);
                }
                else if (isAlphaNumericAlpha(s))
                {
                    result.Add((int)RecognizedTypes.AlphaNumericAlpha, false);
                }
                else if (isNumericAlphaNumeric(s))
                {
                    result.Add((int)RecognizedTypes.NumericAlphaNumeric, false);
                }
            }
            else if (isLetter(s))
            {
                result.Add((int)RecognizedTypes.UndefinedLetters, false);
            }

            //List<string> typesFromOtherDictionaries = predefinedStringTypes.FindTypeNames(s);
            //for (int i = 0; i < typesFromOtherDictionaries.Count; i++)
            //{
            //    string typeName = typesFromOtherDictionaries[i];
            //    //if (typeName == PredefinedStringTypes.CityDictionaryDescription)
            //    //{
            //    //    result.Add((int)RecognizedTypes.City, false);
            //    //}
            //}

            return result;
        }

        public override bool isSeparatorChar(char ch)
        {
            bool result = ch == '-';
            return result;
        }

        public override void Parse(List<string> inputStrings, int maxWordsToParse)
        {
            inputLinesCount = inputStrings.Count;
            ResetValues();
            parse(inputStrings);
        }

        public override void Parse(string inputString, int maxWordsToParse)
        {
            inputLinesCount = 1;
            ResetValues();
            parse(inputString);
        }

        public void ResetValues()
        {
            poBox = "";
            poBoxNumber = "";
            box = "";
            boxNumber = "";
            city = null;
            recipient = null;
            zipCode = null;
            zip9Code = null;
            state = null;
            street = null;
            streetSuffix = null;
            streetNumber = null;
            suite = null;
            preDirection = null;
            postDirection = null;
            poBoxComplete = null;
            boxComplete = null;
            secondaryAddressUnitComplete = null;
            country = null;
            countryDterminedFromOtherAttributes = null;
        }

        private void parse(string inputString)
        {
            List<string> tmp = new List<string>(1); // creating a new list to avoid duplicating the logic...
            tmp.Add(inputString);
            parse(tmp);
        }

        private void parse(List<string> inputStrings)
        {
            shouldEnrichStreet = false;
            List<string> alteredInputStrings = new List<string>(inputStrings.Count);
            for (int i = 0; i < inputStrings.Count; i++)
            {
                string line = inputStrings[i];
                string[] subLines =
                    line.Split(new char[] { ',' },
                        StringSplitOptions.RemoveEmptyEntries); // in address coma is redundant (for now...)
                alteredInputStrings.AddRange(subLines);
            }

            ChoosenPatternList = new ChoosenPatternList(inputWords, (int)PatternIds.UndefinedPattern);
            for (int i = 0; i < alteredInputStrings.Count; i++)
            {
                alteredInputStrings[i] = ensureSpaceIsAfterPoundSign(alteredInputStrings[i]);
            }

            base.Parse(alteredInputStrings, 30);
            getAllMatchedValues(int.MaxValue);

            if (shouldEnrichStreet)
            {
                EnrichStreet();
            }
        }

        private void getAllMatchedValues(int previouslyRemovedCount)
        {
            int removedCount = 0;
            if (state == null)
            {
                state = getMatchedValue(statePatternsGroup, 3, int.MaxValue, PreferedOccurrence.Last,
                    PreferedWidth.Max);
            }

            if (city == null)
            {
                city = getMatchedValue(citiesPatternsGroup, minCity, int.MaxValue, PreferedOccurrence.Last,
                    PreferedWidth.Max);
            }

            if (!isPatternNullOrRemoved(city))
            {
                if (countryAbbreviations.IsMember(city.Text)) // the whole city (not the part of it)
                {
                    removeFromChoosenPatternList(ref city, false, ref removedCount);
                }
            }

            if (country == null)
            {
                country = getMatchedValue(countriesPatternsGroup, 4, int.MaxValue, PreferedOccurrence.Last,
                    PreferedWidth.Max);
            }

            if (city == null) // should stay...
            {
                city = getMatchedValue(citiesPatternsGroup, minCity, int.MaxValue, PreferedOccurrence.Last,
                    PreferedWidth.Max);
            }

            if (poBoxComplete == null)
            {
                poBoxComplete = getMatchedValue(postalBoxPatternsGroup, 0, int.MaxValue, PreferedOccurrence.Last,
                    PreferedWidth.Max);
            }

            if (boxComplete == null)
            {
                boxComplete = getMatchedValue(boxPatternsGroup, 0, int.MaxValue, PreferedOccurrence.Last,
                    PreferedWidth.Max);
            }

            if (streetSuffix == null)
            {
                streetSuffix = getMatchedValue(streetSuffixPatternsGroup, 0, int.MaxValue, PreferedOccurrence.Last,
                    PreferedWidth.Max);
            }

            if (preDirection == null)
            {
                preDirection = getMatchedValue(geoDirectionPatternsGroup, 0, int.MaxValue, PreferedOccurrence.First,
                    PreferedWidth.Min);
            }

            if (postDirection == null)
            {
                postDirection = getMatchedValue(geoDirectionPatternsGroup, 0, int.MaxValue, PreferedOccurrence.Last,
                    PreferedWidth.Min);
            }

            if (zip9Code == null)
            {
                zip9Code = getMatchedValue(usZip9PatternsGroup, 0, int.MaxValue, PreferedOccurrence.Last,
                    PreferedWidth.Max);
            }

            if (secondaryAddressUnitComplete == null)
            {
                secondaryAddressUnitComplete = getMatchedValue(secondaryAddressUnitPatternsGroup, 0, int.MaxValue,
                    PreferedOccurrence.First, PreferedWidth.Max);
            }

            if (streetNumber == null)
            {
                streetNumber = getMatchedValue(streetNumberPatternsGroup, 0, 6, PreferedOccurrence.First,
                    PreferedWidth.Max);
            }

            ProcessExceptionFromAddressParser();

            if (zipCode == null)
            {
                zipCode = getMatchedValue(zipPatternsGroup, 0, int.MaxValue, PreferedOccurrence.Last,
                    PreferedWidth.Max);
                if (zipCode == null)
                {
                    zipCode = getMatchedValue(canadianZipPatternsGroup, 0, int.MaxValue, PreferedOccurrence.Last,
                        PreferedWidth.Max);
                    if (!isPatternNullOrRemoved(zipCode))
                    {
                        countryDterminedFromOtherAttributes = "Canada";
                    }
                }
            }

            splitNumber(PoBoxComplete, ref poBox, ref poBoxNumber);
            splitNumber(BoxComplete, ref box, ref boxNumber);

            ChoosenPatternList.SortAndGroupUnrecognizedWords((int)PatternIds.UndefinedPattern);

            eliminateUnproperOrderedParts(ref removedCount);

            ChoosenPatternList.SortAndGroupUnrecognizedWords((int)PatternIds.UndefinedPattern);

            tryToDetermineUnrecognizedPartsStreetSuite(ref removedCount);

            tryToDetermineUnrecognizedParts();
            if (street == null && poBox == null && recipient != null)
            {
                street = recipient;
                ChoosenPatternList.RemovePermanently(ref recipient, ref removedCount);
            }

            splitNumber(SecondaryAddressUnitComplete, ref secondaryAddressUnit, ref secondaryAddressUnitNumber);
            if (removedCount < previouslyRemovedCount && removedCount > 0)
            {
                getAllMatchedValues(removedCount);
            }
        }

        private void EnrichStreet()
        {
            if (street == null)
            {
                return;
            }

            var replacement = new Dictionary<string, string>()
            {
                { "N", "North" },
                { "E", "East" },
                { "W", "West" },
                { "S", "South" },
                { "NW", "Northwest" },
                { "NE", "Northeast" },
                { "SE", "Southeast" },
                { "SW", "Southwest" }
            };

            string text = street.Text.ToUpper();
            if (replacement.ContainsKey(text))
            {
                street = new ChoosenPattern(street.PatternsGroup, replacement[text], street.StartIndex, street.LastIndex);
            }
        }

        #endregion

        #region misc methods

        private string ensureSpaceIsAfterPoundSign(string s)
        {
            string result = s;
            string pound = "#";
            int position = s.IndexOf(pound);
            if (position >= 0)
            {
                bool
                    notInTheMiddle =
                        false; // # is not in the middle of the word. It is preceeded by space or it is the first char in the whole string
                if (position == 0)
                {
                    notInTheMiddle = true;
                }
                else if (position > 0)
                {
                    notInTheMiddle = s[position - 1] == ' ';
                }

                if (notInTheMiddle)
                {
                    if (s.Length > position + 1)
                    {
                        if (s[position + 1] != ' ')
                        {
                            result = s.Replace("#", "# ");
                        }
                    }
                }
            }

            return result;
        }


        private bool isCanadianZip(string s)
        {
            bool result = s.Length == 6;
            if (result)
            {
                result = char.IsLetter(s[0]) && char.IsDigit(s[1]) && char.IsLetter(s[2]) && char.IsDigit(s[3]) &&
                         char.IsLetter(s[4]) && char.IsDigit(s[5]);
            }

            return result;
        }

        private bool isAlphaNumericAlpha(string s)
        {
            bool result = s.Length == 3;
            if (result)
            {
                result = char.IsLetter(s[0]) && char.IsDigit(s[1]) && char.IsLetter(s[2]);
            }

            return result;
        }

        private bool isNumericAlphaNumeric(string s)
        {
            bool result = s.Length == 3;
            if (result)
            {
                result = char.IsDigit(s[0]) && char.IsLetter(s[1]) && char.IsDigit(s[2]);
            }

            return result;
        }

        /// <summary>
        /// if we found a city before the street number then it is probably something else (e.g. part of the recipient name...)
        /// </summary>
        private void eliminateUnproperOrderedParts(ref int removedCount)
        {
            ChoosenPatternList.RemoveIfBetween(ref city, streetNumber, streetSuffix, ref removedCount);
            ChoosenPatternList.RemoveIfAfter(ref preDirection, streetNumber, ref removedCount);
            ChoosenPatternList.RemoveIfAfter(ref postDirection, streetNumber, ref removedCount);
            ChoosenPatternList.RemoveIfAfter(ref state, poBoxComplete, ref removedCount);
            ChoosenPatternList.RemoveIfAfter(ref state, streetNumber, ref removedCount);
            ChoosenPatternList.RemoveIfAfter(ref state, streetSuffix, ref removedCount);
        }

        /// <summary>
        /// the parts like Recepient and Street cannot be found in dictionary and we try to find them by relative position to other determined parts
        /// </summary>
        override protected void tryToDetermineUnrecognizedParts()
        {
            while (true)
            {
                ChoosenPatternList.AssignLineNumbers();
                bool newDeterminedPatterns = false;
                for (int i = ChoosenPatternList.Count - 1; i >= 0; i--) // some patterns could be removed in the loop
                {
                    ChoosenPattern choosenPattern = ChoosenPatternList[i];
                    newDeterminedPatterns |= determineCity(choosenPattern);
                    newDeterminedPatterns |= determineRecipient(choosenPattern);
                }

                if (!newDeterminedPatterns)
                {
                    break;
                }
            }
        }

        /// <summary>
        /// the parts like Recepient and Street cannot be found in dictionary and we try to find them by relative position to other determined parts
        /// </summary>
        protected void tryToDetermineUnrecognizedPartsStreetSuite(ref int removedCount)
        {
            while (true)
            {
                ChoosenPatternList.AssignLineNumbers();
                bool newDeterminedPatterns = false;
                for (int i = ChoosenPatternList.Count - 1; i >= 0; i--) // some patterns could be removed in the loop
                {
                    ChoosenPattern choosenPattern = ChoosenPatternList[i];
                    newDeterminedPatterns |= determineStreet(choosenPattern);
                    newDeterminedPatterns |= determineSuite(choosenPattern);
                }

                if (street == null)
                {
                    if (streetNumber != null)
                    {
                        removeFromChoosenPatternList(ref streetNumber, true, ref removedCount);
                    }
                }

                if (!newDeterminedPatterns)
                {
                    break;
                }
            }
        }

        private bool determineRecipient(ChoosenPattern choosenPattern)
        {
            bool result = false;
            {
                if (recipient == null)
                {
                    if (choosenPattern.PatternsGroup.PatternId == (int)PatternIds.UndefinedPattern)
                    {
                        if (choosenPattern.StartIndex == 0)
                        {
                            if (choosenPattern.NextPatternId == (int)PatternIds.StreetNumberPattern ||
                                choosenPattern.NextPatternId == (int)PatternIds.SecondaryDesignatorPattern ||
                                choosenPattern.NextPatternId == (int)PatternIds.UndefinedPattern)
                            {
                                choosenPattern.PatternsGroup.PatternId = (int)PatternIds.RecipientPattern;
                                recipient = choosenPattern;
                                result = true;
                            }
                        }
                    }
                }
            }
            return result;
        }

        private bool determineStreet(ChoosenPattern choosenPattern)
        {
            bool result = false;
            if (street == null)
            {
                if (choosenPattern.IsPatternIdBefore((int)PatternIds.StreetNumberPattern) ||
                    choosenPattern.IsFirst ||
                    choosenPattern.IsPatternIdBefore((int)PatternIds.PostalBoxPattern) ||
                    choosenPattern.IsPatternIdBefore((int)PatternIds.StreetSuffixPattern)
                )
                {
                    if (choosenPattern.PatternsGroup.PatternId == (int)PatternIds.UndefinedPattern)
                    {
                        {
                            if (choosenPattern.PreviousPatternId == (int)PatternIds.StreetSuffixPattern)
                            {
                                if (isWithoutLetters(choosenPattern.Text))
                                {
                                    street = choosenPattern;
                                    result = true;
                                }
                            }

                            if (!result)
                            {
                                if (choosenPattern.PreviousPatternId == (int)PatternIds.StreetNumberPattern)
                                {
                                    choosenPattern.PatternsGroup.PatternId = (int)PatternIds.StreetPattern;
                                    street = choosenPattern;
                                    result = true;
                                }

                                if (choosenPattern.IsPreviousInTheSameLine)
                                {
                                    if (choosenPattern.PreviousPatternId == (int)PatternIds.GeoDirectionPattern)
                                    {
                                        choosenPattern.PatternsGroup.PatternId = (int)PatternIds.StreetPattern;
                                        street = choosenPattern;
                                        preDirection = choosenPattern.Previous;
                                        result = true;
                                    }
                                }

                                if (choosenPattern.NextPatternId == (int)PatternIds.GeoDirectionPattern)
                                {
                                    choosenPattern.PatternsGroup.PatternId = (int)PatternIds.StreetPattern;
                                    street = choosenPattern;
                                    postDirection = choosenPattern.Next;
                                    if (preDirection != null)
                                    {
                                        if (preDirection.StartIndex == postDirection.StartIndex)
                                        {
                                            preDirection = null;
                                        }
                                    }

                                    result = true;
                                }
                                else if (choosenPattern.NextPatternId == (int)PatternIds.StreetSuffixPattern)
                                {
                                    choosenPattern.PatternsGroup.PatternId = (int)PatternIds.StreetPattern;
                                    street = choosenPattern;
                                    result = true;
                                }
                            }
                        }
                    }
                }
            }

            return result;
        }

        /// <summary>
        /// does nothing if suite is recognized as "suite 123" or "floor 34" etc.
        /// if suite is only a number or letters then we try to determine it
        /// </summary>
        /// <param name="choosenPattern"></param>
        /// <returns></returns>
        private bool determineSuite(ChoosenPattern choosenPattern)
        {
            bool result = false;
            if (suite == null)
            {
                if (choosenPattern.PatternsGroup.PatternId == (int)PatternIds.UndefinedPattern)
                {
                    if (choosenPattern.IsPreviousInTheSameLine)
                    {
                        if (choosenPattern.PreviousPatternId == (int)PatternIds.StreetNumberPattern ||
                            choosenPattern.PreviousPatternId == (int)PatternIds.GeoDirectionPattern)
                        {
                            choosenPattern.PatternsGroup.PatternId = (int)PatternIds.SecondaryDesignatorPattern;
                            secondaryAddressUnitComplete = choosenPattern;
                            result = true;
                        }
                    }
                }
            }

            return result;
        }

        private bool determineCity(ChoosenPattern choosenPattern)
        {
            bool result = false;
            {
                if (city == null)
                {
                    if (choosenPattern.PatternsGroup.PatternId ==
                         (int)PatternIds.UndefinedPattern &&
                        choosenPattern.StartIndex >= minCity)
                    {
                        if (choosenPattern.NextPatternId == (int)PatternIds.StatePattern)
                        {
                            {
                                choosenPattern.PatternsGroup.PatternId =
                                    (int)PatternIds.CityPattern;
                                city = choosenPattern;
                                result = true;
                            }
                        }
                    }
                }
            }
            return result;
        }

        /// <summary>
        /// this method doesn't check the last string it just assumes it is a number
        /// </summary>
        private void splitNumber(string input, ref string firstPart, ref string number)
        {
            firstPart = "";
            number = "";
            int lastSpaceIndex = input.LastIndexOf(' ');
            if (lastSpaceIndex > 0) // should be always
            {
                firstPart = input.Substring(0, lastSpaceIndex);
                number = input.Substring(lastSpaceIndex + 1);
            }
            else
            {
                firstPart = input;
            }
        }

        /// <summary>
        /// This method was added for processing addresses with structure like "123 East Street"
        /// Customer wants result: '123' - Street Num, 'East' - Street Name , 'Street' - Street Suffix
        /// </summary>
        private void ProcessExceptionFromAddressParser()
        {
            if (street != null) return;

            if (streetNumber != null
                    && (preDirection != null || postDirection != null)
                    && streetSuffix != null)
            {
                if (preDirection != null)
                {
                    if (streetNumber.StartIndex + 1 == preDirection.StartIndex && preDirection.StartIndex + 1 == streetSuffix.StartIndex)
                    {
                        street = preDirection;
                        preDirection = null;
                        shouldEnrichStreet = true;
                    }
                    else if (postDirection != null)
                    {
                        if (streetNumber.StartIndex + 1 == postDirection.StartIndex && postDirection.StartIndex + 1 == streetSuffix.StartIndex)
                        {
                            street = postDirection;
                            postDirection = null;
                        }
                    }
                }
            }
        }
        #endregion

        #endregion
    }
}
