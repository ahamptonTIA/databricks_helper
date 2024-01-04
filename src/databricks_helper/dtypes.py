import json, ast

#---------------------------------------------------------------------------------- 

class dict:
  
  """Class for org of dictionary related functions"""
  
  def eval_nested_string_literals(self, data):
      """
      Iterates through a nested dictionary or JSON object, attempting to evaluate
      string representations of data types (e.g., lists, dict, tuples) into their 
      actual Python counterparts. Modifies the structure in-place, replacing string
      representations with evaluated values.
  
      Parameters:
      ----------
      data : dict or str
          The nested dictionary to iterate through, or a JSON string to be parsed.
      Returns:
      -------
      dict : dict
          The modified dictionary with string representations replaced by evaluated
          values.
      """   
      if isinstance(data, str):
          data = json.loads(data)  # Parse JSON string if needed
      for k, v in data.items():
          if isinstance(v, dict):
              self.eval_nested_string_literals(v)  # Recursive call for nested dictionaries
          else:
              try:
                  x = ast.literal_eval(v)
                  if x:  # Replace only if evaluation is successful
                      data[k] = x  # Replace the value in the dictionary
              except:
                  pass  # Ignore evaluation errors
      return data

#---------------------------------------------------------------------------------- 
