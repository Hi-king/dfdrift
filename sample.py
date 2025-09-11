import pandas as pd
import dfvalidate

def main():
    df1 = pd.DataFrame({
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35],
        'city': ['Tokyo', 'Osaka', 'Kyoto']
    })
    
    dfvalidate.validate(df1)
    print("Validated df1")
    
    df2 = pd.DataFrame({
        'id': [1, 2, 3],
        'score': [85.5, 92.0, 78.5]
    })
    
    dfvalidate.validate(df2)
    print("Validated df2")

if __name__ == "__main__":
    main()