import pandas as pd
import dfvalidate

def main():
    custom_validator = dfvalidate.DfValidator(
        storage=dfvalidate.LocalFileStorage("./custom_schemas")
    )
    
    df = pd.DataFrame({
        'product': ['A', 'B', 'C'],
        'price': [100, 200, 300]
    })
    
    custom_validator.validate(df)
    print("Validated with custom storage path")

if __name__ == "__main__":
    main()