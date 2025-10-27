from langchain_openai import ChatOpenAI
from langchain_core.messages import SystemMessage, HumanMessage
from dotenv import load_dotenv
import os
import json

load_dotenv()
llm = ChatOpenAI(model="gpt-4o-mini", api_key=os.getenv("OPENAI_API_KEY"))
# DIRECTORY = os.path.dirname(os.path.abspath(__file__))

def make_content_of_one_place(place_name : str, address : str, review_texts : list[str]) -> str:
    messages = [
        SystemMessage(content="You are a helpful assistant that can help me fill in the content of a place. You must answer in Korean"),
        HumanMessage(content=f"The place name is {place_name}, the address is {address}, and the review texts are {review_texts}. \
Based on the reviews, please write a summarized description about the place itself. \
The content should include the overall characteristics and atmosphere of the place, as well as the positive aspects (Good points) and negative aspects (Bad points) perceived by users. \
Please ensure that the output uses appropriate spacing and paragraph breaks, and clearly separates the advantages and disadvantages in the following format: \
\n\
(summary...)  \\n\
\
**장점:**  \
- good_point_1  \
- good_point_2  \
...  \
**단점:**  \
- bad_point_1  \
- bad_point_2  \
...  \
\n\
Write the entire content in Korean, with a total length of less than 500 words, and format it in React Markdown Viewer style.")
    ]
    response = llm.invoke(messages)
    return response.content


def fill_contents(tmp_path : str) -> None:
    # 디렉토리 생성
    filled_contents_dir = os.path.join(tmp_path, "filled-contents")
    crawled_reviews_dir = os.path.join(tmp_path, "crawled-reviews")
    os.makedirs(filled_contents_dir, exist_ok=True)
    
    files = os.listdir(crawled_reviews_dir)
    
    for file in files:
        with open(os.path.join(crawled_reviews_dir, file), "r", encoding="utf-8") as f:
            record = json.loads(f.read())
        
        if(record == {}):
            continue
        place_no = record["place_no"]
        reviews = record["reviews"]
        if(len(reviews) == 0):
            continue
        place_name = reviews[0]["place_name"]
        address = reviews[0]["address"]
        review_texts = [review["review_text"] for review in reviews]
        content = make_content_of_one_place(place_name, address, review_texts)
        place_obj = {
            "place_no": place_no,
            "content": content
        }
        with open(os.path.join(filled_contents_dir, file), "w", encoding="utf-8") as f:
            f.write(json.dumps(place_obj, ensure_ascii=False) + "\n")


# fill_contents()
