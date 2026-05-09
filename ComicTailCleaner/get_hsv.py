import sys
from PIL import Image

sys.path.append(r"c:\Users\dx90c\Desktop\Python腳本集合\ComicTailCleaner_v16.0.3-加掛模式 - 複製")

img1_path = r"c:\Users\dx90c\Desktop\Python腳本集合\ComicTailCleaner_v16.0.3-加掛模式 - 複製\廣告比對資料夾\ee (26).jpg"
img2_path = r"c:\Users\dx90c\Desktop\Python腳本集合\ComicTailCleaner_v16.0.3-加掛模式 - 複製\廣告比對資料夾\038.webp"

def calc_hsv(path):
    import colorsys
    img = Image.open(path).convert('RGB').resize((100, 100))
    pixels = img.load()
    w, h = img.size
    h_sum, s_sum, v_sum = 0, 0, 0
    count = 0
    for y in range(h):
        for x in range(w):
            r, g, b = pixels[x, y]
            hx, sx, vx = colorsys.rgb_to_hsv(r/255.0, g/255.0, b/255.0)
            h_sum += hx; s_sum += sx; v_sum += vx
            count += 1
    return (h_sum/count*360, s_sum/count, v_sum/count)

hsv1 = calc_hsv(img1_path)
hsv2 = calc_hsv(img2_path)
print("HSV1:", hsv1)
print("HSV2:", hsv2)
print("V diff:", abs(hsv1[2] - hsv2[2]))
