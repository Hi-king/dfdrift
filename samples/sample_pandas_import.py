import dfvalidate.pandas as pd
import pandas as pd_original
from datetime import datetime

def main():
    pd.configure_validation()
    
    print("Using dfvalidate.pandas as pd")
    
    df1 = pd.DataFrame({
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35],
        'city': ['Tokyo', 'Osaka', 'Kyoto']
    })
    print("Created df1 - schema automatically saved")
    
    df2 = pd.DataFrame.from_dict({
        'id': [1, 2, 3],
        'score': [85.5, 92.0, 78.5]
    })
    print("Created df2 using from_dict - schema automatically saved")
    
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    df3 = pd.DataFrame({
        'name': ['Alice', 'Bob'],
        current_time: [1, 2]
    })
    print(f"Created df3 with timestamp column '{current_time}' - schema automatically saved")
    
    # Test original pandas - should NOT be logged
    df_original = pd_original.DataFrame({
        'name': ['Charlie', 'David'],
        'age': [35, 40]
    })
    print("Created df with original pandas - should NOT be logged")
    
    print(f"dfvalidate.pandas DataFrame type: {type(df1)}")
    print(f"Original pandas DataFrame type: {type(df_original)}")
    print(f"DataFrame shape: {df1.shape}")
    print(f"Pandas version: {pd.__version__}")

if __name__ == "__main__":
    main()