# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, udf, ceil, percent_rank,from_json, explode,ntile, desc, asc, round, when, lit, pow, current_timestamp, current_date, log, monotonically_increasing_id, row_number, concat, array, struct, rank, dense_rank,  from_json, to_json, collect_list, transform,flatten, broadcast, regexp_replace, concat_ws, avg, exists, abs, coalesce, count, split, array_sort, array_join, upper

from pyspark.sql.types import *
from pyspark.sql.window import Window
import torch
from sentence_transformers import SentenceTransformer, util
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import LabelEncoder, StandardScaler
from tensorflow.keras.layers import Embedding, Input, Flatten, Concatenate, Dense
from tensorflow.keras.models import Model
import numpy as np
import pandas as pd
import pyspark.sql.functions as f
import sys
import scipy
import json
from functools import reduce
from decimal import Decimal
import hashlib
from delta.tables import DeltaTable
import requests

from colormath.color_conversions import convert_color
from colormath.color_objects import LabColor, sRGBColor
from colormath.color_diff import delta_e_cmc

print("Python version:", sys.version)

# COMMAND ----------

class ps_metrics:
    def __init__(self):
        # Most relevant features for metrics by product group
        self.relevant_attributes = {
        'Outerwear': ['GENDER', 'COLOR', 'TYPE', 'DECORATION_TECHNOLOGY_1', 'DECORATION_LOCATION_1'],
        'Polos': ['GENDER', 'COLOR', 'DECORATION_TECHNOLOGY_1', 'SLEEVE_STYLE','DECORATION_LOCATION_1'],
        'T-shirts': ['GENDER', 'COLOR', 'DECORATION_TECHNOLOGY_1', 'SLEEVE_STYLE', 'DECORATION_LOCATION_1'],
        'Sweaters': ['GENDER', 'COLOR', 'TYPE', 'DECORATION_TECHNOLOGY_1', 'DECORATION_LOCATION_1'],
        'Dress Shirts': ['GENDER', 'COLOR', 'DECORATION_TECHNOLOGY_1', 'SLEEVE_STYLE', 'DECORATION_LOCATION_1'],
        'Sportswear': ['GENDER', 'COLOR', 'DECORATION_TECHNOLOGY_1', 'SLEEVE_STYLE', 'DECORATION_LOCATION_1'],
        'Bottoms': ['GENDER', 'COLOR', 'TYPE', 'DECORATION_TECHNOLOGY_1', 'DECORATION_LOCATION_1'],
        'Hats': ['FIT_TYPE', 'COLOR', 'TYPE', 'DECORATION_TECHNOLOGY_1', 'DECORATION_LOCATION_1'],
        'Beanies': ['FIT_TYPE', 'COLOR', 'TYPE', 'DECORATION_TECHNOLOGY_1', 'DECORATION_LOCATION_1']}

        # defining correctness with groupings for broad categories
        # TYPE 
        self.outerwear_types = {"SOFTSHELL", "JACKET", "INSULATED", "LIGHTWEIGHT", "JACKET, LIGHTWEIGHT","JACKET, FLEECE & KNITS","3-IN-1", "JACKET, SOFTSHELL", "FLEECE JACKETS & KNITS", "OUTERWEAR"}
        self.sweatshirt_types = {"SWEATER", "FLEECE & KNITS", "SWEATSHIRT", "SWEATSHIRT, FLEECE & KNITS", "CARDIGAN", "JACKET, FLEECE & KNITS","HOODIES", "FLEECE JACKETS & KNITS", "SWEATERS & SWEATSHIRTS", "SWEATERS"}
        self.dressshirt_types = {"DRESS SHIRT", "WOVEN SHIRT","BLOUSE","WORK WEAR","WORK SHIRT","WORKWEAR", "SHIRT"}

        self.type_inclusion = {'SOFTSHELL': self.outerwear_types
                    , 'INSULATED': self.outerwear_types
                    , 'JACKET': self.outerwear_types
                    , 'LIGHTWEIGHT': self.outerwear_types
                    , 'JACKET, LIGHTWEIGHT': self.outerwear_types
                    , '3-IN-1': self.outerwear_types
                    , 'JACKET, SOFTSHELL': self.outerwear_types
                    , 'OUTERWEAR': self.outerwear_types
                    , 'SWEATER': self.sweatshirt_types
                    , 'FLEECE & KNITS': self.sweatshirt_types
                    , 'SWEATSHIRT, FLEECE & KNITS': self.sweatshirt_types
                    , 'SWEATSHIRT': self.sweatshirt_types
                    , 'SWEATERS & SWEATSHIRTS': self.sweatshirt_types
                    , 'SWEATERS': self.sweatshirt_types
                    , 'CARDIGAN': self.sweatshirt_types
                    , 'HOODIES': self.sweatshirt_types
                    , 'JACKET, FLEECE & KNITS': self.sweatshirt_types | self.outerwear_types
                    , 'FLEECE JACKETS & KNITS': self.sweatshirt_types | self.outerwear_types
                    , 'T-SHIRT': {'T-SHIRT','T SHIRTS', "SHIRT", 'T-SHIRTS'}
                    , 'T SHIRTS': {'T-SHIRT','T SHIRTS', "SHIRT", 'T-SHIRTS'}
                    , 'T-SHIRTS': {'T-SHIRT','T SHIRTS', "SHIRT", 'T-SHIRTS'}
                    , 'SHIRT': {'T-SHIRT','T SHIRTS', "SHIRT"} | self.dressshirt_types
                    , 'POLO SHIRT': {'POLO SHIRT', 'POLOS'}
                    , 'POLOS': {'POLO SHIRT', 'POLOS'}
                    , 'TANK TOP': {'TANK TOP'}
                    , 'WIND SHIRT': {'WIND SHIRT'}
                    , 'VEST': {'VEST'}
                    , 'BODY WARMER': {'BODY WARMER'}
                    , 'BLAZER': {'BLAZER'}
                    , 'BASEBALL CAPS': {'BASEBALL CAPS', 'BASEBALL CAP', 'CAP'}
                    , 'BASEBALL CAP': {'BASEBALL CAPS', 'BASEBALL CAP', 'CAP'}
                    , 'TRUCKER CAPS': {'TRUCKER CAPS', 'CAP'}
                    , 'CAP': {'CAP', 'TRUCKER CAPS', 'BASEBALL CAPS', 'BASEBALL CAP'}
                    , 'BEANIE': {'BEANIE'}
                    , 'VISORS': {'VISORS'}
                    , 'BRIM HAT': {'BRIM HAT'}
                    , 'DRESS SHIRT': self.dressshirt_types
                    , 'WOVEN SHIRT': self.dressshirt_types
                    , 'BLOUSE': self.dressshirt_types
                    , 'WORK WEAR': self.dressshirt_types
                    , 'WORK SHIRT': self.dressshirt_types
                    , 'WORKWEAR': self.dressshirt_types
                    , 'SHORTS': {'SHORTS'}
                    , 'PANTS': {'PANTS', 'BOTTOMS'}
                    , 'BOTTOMS': {'PANTS', 'BOTTOMS'}
                    , 'LEGGINGS': {'LEGGINGS'}
                    , 'BABY HAT': {'BABY HAT'}
                    , 'HEADBAND': {'HEADBAND'}
                    , 'TRACK': {'TRACK'}
                    , 'FASHION': {'FASHION'}
                    }
        

        # DECO TECH
        self.screen_print = {'SCREEN PRINTING','SINGLE COLOR', 'SINGLE COLOR PRINT'}
        self.direct_to_garment = {'COLOR PRINT','FULL COLOR TRANSFER','DIRECT GARMENT PRINT','FULL COLOR PRINT', 'FULL COLOR', 'DTF'}
        self.heat_transfer = {'HEAT TRANSFER'}
        self.embroidery = {'EMBROIDERY'}
        self.dye_sublimation = {'SUBLIMATION', 'DYE SUBLIMATION'}
        self.digital = {'DIGITAL', 'DIGITAL PRINTING'}

        
        self.deco_tech_inclusion = {'SCREEN PRINTING': self.screen_print
                    , 'SINGLE COLOR': self.screen_print
                    , 'SINGLE COLOR PRINT': self.screen_print
                    , 'COLOR PRINT': self.direct_to_garment
                    , 'FULL COLOR TRANSFER': self.direct_to_garment
                    , 'DIRECT GARMENT PRINT': self.direct_to_garment
                    , 'FULL COLOR PRINT': self.direct_to_garment
                    , 'FULL COLOR': self.direct_to_garment
                    , 'HEAT TRANSFER': self.heat_transfer
                    , 'EMBROIDERY': self.embroidery
                    , 'SUBLIMATION': self.dye_sublimation
                    , 'DYE SUBLIMATION': self.dye_sublimation
                    , 'DIGITAL': self.digital
                    , 'DIGITAL PRINTING': self.digital 
                    , 'PROMOV3': {'PROMOV3'}
                    , 'PAD PRINTING': {'PAD PRINTING'}
                    , 'ENGRAVING': {'ENGRAVING'}
                    }


        # DECO LOCATION
        self.back_deco = {"BACK", "FULL BACK", "CAP BACK", 'CENTERED ON BACK', 'ON BACK', 'CENTRE BACK PANEL','FRONT AND BACK','BACK SIDE','BACK, IMPACT UPPER BACK', 'IMPACT UPPER BACK','BACK CENTRE', 'LEFT CHEST + BACK','BACK YOKE,  HORIZONTAL, CENTERED ON YOKE','BACK,  HORIZONTAL, CENTERED ON BACK', 'BACK,  HORIZONTAL, CENTERED ON BACK', 'BACK YOKE,  HORIZONTAL, CENTERED ON YOKE'}
        self.front_deco = {"CENTERED ON FRONT", "FRONT", "FULL CHEST", "FULL FRONT", "ON FRONT", "FRONT-CENTRAL", "FRONT HAT", "FRONT PANEL", "FRONT PANEL,  CENTER OF DECO AREA 9 CM FROM SIDE SEAM AND 5 CM FROM BOTTOM EDGE", "CENTER FRONT", "CUFFED CENTER FRONT", 'FRONT, CENTERED', "FRONT CENTER", 'FRONT, CENTERED ON UPPER PANEL', 'FULL FRONT ONLY', 'FULL-FRONT', 'CAP,  SIDE OPPOSITE FF LONG SEAM, ON DOUBLE FOLDED EDGE','FRONT/CENTERED','FRONT AND BACK','CENTRE FRONT','FRONT CHEST','FRONT BOTTOM','CENTERED ONN CAP FRONT,  CENTER OF DECORATION AREA 4.5 CM ABOVE LOWER FRONT PANEL SEAM.','ON FRONT,  CENTER OF DECO AREA 4.5 CM ABOVE SEAM AND 6.5 FROM SIDE SEAM','FRONT CENTRE','CHEST,  HORIZONTAL, CENTERED ON FRONT ACROSS CHEST BELOW PLACKET', 'CHEST,  HORIZONTAL, CENTERED ON FRONT ACROSS CHEST BELOW PLACKET'}
        self.left_deco = {"CENTERED ON LEFT CHEST", "LEFT CHEST", "CENTERED ON POCKET", "LC", "CENTERED ON LOWER LEFT LEG", "ON LEFT THIGH", "LEFT", "CAP LEFT", "LEFT LEG", "ON LEFT LEG", "CENTERED ON UPPER LEFT LEG (OFF TO THE RIGHT)", "CENTERED UPPER LEFT LEG 100MM UNDER THE SEAM", "CENTERED ON LEFT THIGH", "LEFT HIP", 'THIGH VERTICAL,  CENTERED ON LEFT THIGH', 'LEFT LEG / ON TOP NEAR POCKET', 'THIGH VERTICAL / CENTERED ON LEFT THIGH', 'CENTERED ON LEFT LEG POCKET', 'CENTERED LEFT BETWEEN STRIPES','CHEST,  HORIZONTAL, CENTERED ON LEFT CHEST', 'ON LEFT LEG,  CENTER OF DECO AREA 10 CM BELOW POCKET LIMIT', 'LEFT PANEL, CENTERED', 'ON SIDE', 'CENTERED ON LEFT LOWER POCKET,  UNDER BELT','ON POCKET','CHEST, HORIZONTAL,  - CENTERED ON LEFT CHEST','LEFT CHEST + SLEEVES','LEFT LEG,  CENTERED ON TOP NEAR POCKET','CENTERED UPPER LEFT LEG','POCKET FONT','TOP LEFT LEG','CENTERED ON  LEFT LEG POCKET,  CENTER OF DECO AREA  87MM  ABOVE POCKET BOTTOM','CENTERED ON CUFF,  CENTER OF DECO AREA 3, 5 CM FROM UPPER CUFF EDGE AND 10 CM FROM SIDE CUFF EDGE','CENTERED ABOVE POCKET,  DECO AREA STARTS 2CM ABOVE POCKET','CENTERED ON LEFT CHEST ABOVE POCKET','BOTTOM LEFT LEG','NEAR LEFT LEG POCKET', 'LEFT CHEST + BACK', "POCKET FRONT",'CHEST,  CENTRED ON LEFT CHEST', 'UPPER LEFT LEG', 'CHEST,  CENTRED ON LEFT CHEST'}
        self.right_deco = {"RIGHT CHEST", "CENTERED ON POCKET", "CENTERED ON RIGHT CHEST", "RIGHT FRONT ABOVE SEAM", "RIGHT FRONT ABOVE SEAM (SLANTED)", "RIGHT YOKE", "RIGHT SLEEVE", "CENTERED ON RIGHT PANEL", "FRONT CENTERED RIGHT LEG", 'CENTERED ON STRAP / SIDE OPPOSITE OF LONG SEAM,  LOGO ELEVATE ON RIGHT SIDE', 'CENTERED ON RIGHT LEG', 'RIGHT LEG SIDE POCKET', 'ON SIDE','ON POCKET','POCKET FONT','CENTERED ON CUFF,  CENTER OF DECO AREA 3, 5 CM FROM UPPER CUFF EDGE AND 10 CM FROM SIDE CUFF EDGE','CENTERED ABOVE POCKET, DECO AREA STARTS 2CM ABOVE POCKET', '''SIDE OPPOSITE OF LONG SEAM,  LOGO ''ELEVATE'' ON RIGHT SIDE, ON BODY''', "POCKET FRONT"}

        self.deco_location_inclusion = {"BACK": self.back_deco,
            "CENTERED ON FRONT": self.front_deco,
            "CENTERED ON LEFT CHEST":  self.left_deco,
            "CENTERED ON POCKET":  self.left_deco | self.right_deco,
            "CENTERED ON RIGHT CHEST": self.right_deco,
            "EMBROIDERY": self.back_deco | self.front_deco | self.left_deco | self.right_deco ,
            "FRONT": self.front_deco,
            "FULL BACK": self.back_deco,
            "FULL CHEST": self.front_deco,
            "FULL FRONT": self.front_deco,
            "LC":  self.left_deco,
            "LEFT CHEST":  self.left_deco,
            "ON FRONT": self.front_deco,
            "RIGHT CHEST": self.right_deco,
            "RIGHT FRONT ABOVE SEAM": self.right_deco,
            "RIGHT FRONT ABOVE SEAM (SLANTED)": self.right_deco,
            "RIGHT YOKE": self.right_deco,
            "FRONT-CENTRAL": self.front_deco,
            "FRONT HAT": self.front_deco,
            "FRONT PANEL": self.front_deco,
            "CENTERED ON LOWER LEFT LEG": self.left_deco,
            "RIGHT SLEEVE": self.right_deco,
            "FRONT PANEL,  CENTER OF DECO AREA 9 CM FROM SIDE SEAM AND 5 CM FROM BOTTOM EDGE": self.front_deco,
            "CENTERED ON RIGHT PANEL": self.right_deco,
            "ON LEFT THIGH": self.left_deco,
            "LEFT": self.left_deco,
            "CAP LEFT": self.left_deco,
            "LEFT LEG": self.left_deco,
            "ON LEFT LEG": self.left_deco,
            "CENTERED ON UPPER LEFT LEG (OFF TO THE RIGHT)": self.left_deco,
            "CENTERED UPPER LEFT LEG 100MM UNDER THE SEAM": self.left_deco,
            "CUFFED CENTER FRONT": self.front_deco,
            "CENTERED ON LEFT THIGH": self.left_deco,
            "LEFT HIP": self.left_deco,
            "CAP BACK": self.back_deco,
            "RIGHT YOKE": self.right_deco,
            "CENTER FRONT": self.front_deco,

            "FRONT, CENTERED": self.front_deco,
            "FRONT CENTERED RIGHT LEG": self.right_deco,
            "FRONT CENTER": self.front_deco,
            "FRONT, CENTERED ON UPPER PANEL": self.front_deco,
            "THIGH VERTICAL,  CENTERED ON LEFT THIGH": self.left_deco,
            "LEFT LEG / ON TOP NEAR POCKET": self.left_deco,
            "THIGH VERTICAL / CENTERED ON LEFT THIGH": self.left_deco,
            "FULL FRONT ONLY": self.front_deco,
            "CENTERED ON BACK": self.back_deco,
            "CENTERED ON STRAP / SIDE OPPOSITE OF LONG SEAM,  LOGO ELEVATE ON RIGHT SIDE": self.right_deco,
            "CENTERED ON LEFT LEG POCKET": self.left_deco,
            "FULL-FRONT": self.front_deco,
            "CENTERED ON RIGHT LEG": self.right_deco,
            "ON BACK": self.back_deco,
            "RIGHT LEG SIDE POCKET": self.right_deco,
            "CENTRE BACK PANEL": self.back_deco,
            "CENTERED LEFT BETWEEN STRIPES": self.left_deco,
            "CHEST,  HORIZONTAL, CENTERED ON LEFT CHEST": self.left_deco,
            "ON LEFT LEG,  CENTER OF DECO AREA 10 CM BELOW POCKET LIMIT": self.left_deco,
            "CAP,  SIDE OPPOSITE FF LONG SEAM, ON DOUBLE FOLDED EDGE": self.front_deco,
            "LEFT PANEL, CENTERED": self.left_deco,
            "ON SIDE": self.left_deco | self.right_deco,
            "CENTERED ON LEFT LOWER POCKET,  UNDER BELT": self.left_deco,
            "FRONT/CENTERED": self.front_deco,
            "FRONT AND BACK": self.front_deco | self.back_deco,
            "SIDE OPPOSITE OF LONG SEAM,  LOGO ''ELEVATE'' ON RIGHT SIDE ,ON BODY": self.right_deco,
            "ON POCKET": self.left_deco | self.right_deco,
            "CHEST,  HORIZONTAL,  - CENTERED ON LEFT CHEST": self.left_deco,
            "BACK SIDE": self.back_deco,
            "LEFT CHEST + SLEEVES": self.left_deco,
            "CENTRE FRONT": self.front_deco,
            "LEFT LEG,  CENTERED ON TOP NEAR POCKET": self.left_deco,
            "CENTERED UPPER LEFT LEG": self.left_deco,
            "BACK, IMPACT UPPER BACK": self.back_deco,
            "POCKET FONT": self.left_deco | self.right_deco,
            "IMPACT UPPER BACK": self.back_deco,
            "TOP LEFT LEG": self.left_deco,
            "BACK CENTRE": self.back_deco,
            "CENTERED ON  LEFT LEG POCKET,  CENTER OF DECO AREA  87MM  ABOVE POCKET BOTTOM": self.left_deco,
            "FRONT BOTTOM": self.front_deco,
            "CENTERED ON CUFF,  CENTER OF DECO AREA 3, 5 CM FROM UPPER CUFF EDGE AND 10 CM FROM SIDE CUFF EDGE": self.left_deco | self.right_deco,
            "CENTERED ONN CAP FRONT,  CENTER OF DECORATION AREA 4.5 CM ABOVE LOWER FRONT PANEL SEAM.": self.front_deco,
            "CENTERED ABOVE POCKET,  DECO AREA STARTS 2CM ABOVE POCKET": self.left_deco,
            "FRONT CENTRE": self.front_deco,
            "CENTERED ON LEFT CHEST ABOVE POCKET": self.left_deco,
            "BOTTOM LEFT LEG": self.left_deco,
            "NEAR LEFT LEG POCKET": self.left_deco,
            '''SIDE OPPOSITE OF LONG SEAM,  LOGO ''ELEVATE'' ON RIGHT SIDE, ON BODY''': self.right_deco,
            "FRONT CHEST": self.left_deco,
            "POCKET FRONT": self.left_deco | self.right_deco,
            "LEFT CHEST + BACK": self.left_deco | self.back_deco,
            "ON FRONT,  CENTER OF DECO AREA 4.5 CM ABOVE SEAM AND 6.5 FROM SIDE SEAM": self.front_deco,
            "UPPER LEFT LEG": self.left_deco,
            "CHEST,  CENTRED ON LEFT CHEST": self.left_deco,
            "BACK,  HORIZONTAL, CENTERED ON BACK": self.back_deco, 
            "BACK YOKE,  HORIZONTAL, CENTERED ON YOKE": self.back_deco,
            "CHEST,  HORIZONTAL, CENTERED ON FRONT ACROSS CHEST BELOW PLACKET": self.front_deco
            }
        
        # SLEEVE STYLE
        self.sleeve_style_inclusion = {'3/4 SLEEVE': {'3/4 SLEEVE','HALF SLEEVE'},
                     'HALF SLEEVE': {'3/4 SLEEVE','HALF SLEEVE'},
                     'LONG SLEEVE': {'LONG SLEEVE'},
                     'SHORT SLEEVE': {'SHORT SLEEVE'},
                     'SLEEVELESS': {'SLEEVELESS','SLEEVLESS'},
                     'SLEEVLESS': {'SLEEVELESS','SLEEVLESS'},
                     'ABC': {'ABC'}}
        

        # DEFINED VALUES
        self.defined_values = {'GENDER': {'MEN','MEN OR WOMEN','WOMEN','YOUTH','BABY',None}
                               , 'TYPE': {t.upper() for t in self.type_inclusion}
                               , 'DECORATION_TECHNOLOGY_1': {d.upper() for d in self.deco_tech_inclusion}
                               , 'DECORATION_LOCATION_1': {d.upper() for d in self.deco_location_inclusion}
                               , 'SLEEVE_STYLE': {s.upper() for s in self.sleeve_style_inclusion}
                               , 'FIT_TYPE': {'ADJUSTABLE', 'STRETCH FIT', 'ONE SIZE', 'FITTED', 'REGULAR FIT'}
        }

    def irrelevant_attribute_check(self, product_group, metric):
        # if metric isn't relevant to product group, return True
        if metric not in self.relevant_attributes[product_group]:
            return True
    
    def missing_value_check(self, attrib1, attrib2, defined_values):
        # if either attribute is null or not in defined values, return True
        if attrib1 is None or attrib2 is None:
            return True

        attrib1 = attrib1.upper()
        attrib2 = attrib2.upper()

        return attrib1 not in defined_values or attrib2 not in defined_values
    
    def undefined_values(self, df):
        undefined_vals = []
        distinct_attribs = set(val for sublist in self.relevant_attributes.values() for val in sublist)
        cols_to_check = [i for i in distinct_attribs if i != 'COLOR']
        for i in cols_to_check:
            distinct_vals = df.select(f"{i}").distinct().rdd.flatMap(lambda x: x).collect()
            for val in distinct_vals:
                if val is not None and val.upper() not in self.defined_values[i]:
                    undefined_vals.append((i, val))
        return(undefined_vals)
        
    def gender_precision(self, product_group, gender1, gender2):
        #relevancy check
        if self.irrelevant_attribute_check(product_group, 'GENDER'):
            return None

        # missing values handling 
        if self.missing_value_check(gender1, gender2, self.defined_values['GENDER']):
            return None 
        
        # normal value evaluation logic
        # any part part of the gender matches
        return 1 if set(gender1.upper().split()) & set(gender2.upper().split()) else 0
    
    def type_precision(self, product_group, type1, type2):
        #relevancy check
        if self.irrelevant_attribute_check(product_group, 'TYPE'):
            return None

        # missing values handling 
        if self.missing_value_check(type1, type2, self.defined_values['TYPE']):
            return None
            
        # normal value evaluation logic
        # type is in acceptable types defined above
        return 1 if type2.upper() in self.type_inclusion.get(type1.upper(), set()) else 0
    
    def deco_tech_precision(self, product_group, deco_tech1, deco_tech2):
        #relevancy check
        if self.irrelevant_attribute_check(product_group, 'DECORATION_TECHNOLOGY_1'):
            return None

        # missing values handling 
        if self.missing_value_check(deco_tech1, deco_tech2, self.defined_values['DECORATION_TECHNOLOGY_1']):
            return None

        # normal value evaluation logic
        # deco tech is in acceptable deco techs defined above
        return 1 if deco_tech2.upper() in self.deco_location_inclusion.get(deco_tech1.upper(), set())  else 0
    
    def deco_location_precision(self, product_group, deco_location1, deco_location2):
        #relevancy check
        if self.irrelevant_attribute_check(product_group, 'DECORATION_LOCATION_1'):
            return None

        # missing values handling 
        if self.missing_value_check(deco_location1, deco_location2, self.defined_values['DECORATION_LOCATION_1']):
             return None
         
        # normal value evaluation logic
        # deco location is in acceptable deco locations defined above
        return 1 if deco_location2.upper() in self.deco_location_inclusion.get(deco_location1.upper(), set())  else 0
    
    def sleeve_style_precision(self, product_group, sleeve_style1, sleeve_style2):
        #relevancy check
        if self.irrelevant_attribute_check(product_group, 'SLEEVE_STYLE'):
            return None

        # missing values handling 
        if self.missing_value_check(sleeve_style1, sleeve_style2, self.defined_values['SLEEVE_STYLE']):
            return None

        # normal value evaluation logic
        # sleeve style is in acceptable sleeve styles defined above
        return 1 if sleeve_style2.upper() in self.sleeve_style_inclusion.get(sleeve_style1.upper(), set()) else 0
    
    def fit_type_precision(self, product_group, fit_type1, fit_type2):
        #relevancy check
        if self.irrelevant_attribute_check(product_group, 'FIT_TYPE'):
            return None

        # missing values handling 
        if self.missing_value_check(fit_type1, fit_type2, self.defined_values['FIT_TYPE']):
            return None

        # normal value evaluation logic
        # fit type match
        return 1 if fit_type1.upper() == fit_type2.upper() else 0
    
    def color_similarity(self, product_group, color1, color2):
        #relevancy check
        if self.irrelevant_attribute_check(product_group, 'COLOR'):
            return None
        
        import numpy
        def patch_asscalar(a):
            return a.item()
        setattr(numpy, "asscalar", patch_asscalar)

        c1_rgb = sRGBColor.new_from_rgb_hex(color1)
        c2_rgb = sRGBColor.new_from_rgb_hex(color2)

        c1_lab = convert_color(c1_rgb, LabColor)
        c2_lab = convert_color(c2_rgb, LabColor)
        delta_e = delta_e_cmc(c1_lab, c2_lab)

        # convert to 0-1 range and reverse delta_e scale
        color_metric = 1 - (delta_e / 100)

        return color_metric 
        

# COMMAND ----------

    def gender_precision(self, product_group, gender1, gender2):
        #relevancy check
        if self.irrelevant_attribute_check(product_group, 'GENDER'):
            return None

        # missing values handling 
        if self.missing_value_check(gender1, gender2, self.defined_values['GENDER']):
            return None 
        
        # normal value evaluation logic
        # any part part of the gender matches
        return 1 if set(gender1.upper().split()) & set(gender2.upper().split()) else 0
